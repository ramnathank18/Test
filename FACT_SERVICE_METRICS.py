# ===== Minimal Databricks SQL → Delta Writer =====
# Purpose: Run raw SELECT and save output as Delta table.
# No transformations. No rewrites. No widgets. No checks.

from pyspark.sql.dataframe import DataFrame

# Set catalog so two-part names resolve the same way as SQL Editor
##spark.sql("USE CATALOG biprodcatalog")
catalog = dbutils.widgets.get("catalog_name")
spark.sql(f"USE CATALOG {catalog}")


# -------- Paste your FULL working SQL query here EXACTLY as in SQL Editor --------
sql_text = """WITH FACT_SERVICE_METRICS AS (
SELECT
	HISTORY_TYPE,
	-- CAST(WEEK_NUMBER AS INT) AS WEEK_NUMBER,
    CAST(PROM_WEEK.MCAL_WEEK AS INT) AS WEEK_NUMBER,
	TRIP_ID,
	STOP_NUMBER,
	CUSTOMER,
	CUST_PO,
	ORDER_NUMBER,
	LINE_NUMBER,
	CAST(DELIVERY_NUMBER AS INT) AS DELIVERY_NUMBER,
	FREIGHT_METHOD,
	ORGANIZATION_CODE,
	ITEM,
	UOM,
	CAST(QTY_REQUESTED AS DOUBLE) AS QTY_REQUESTED,
	CAST(PALLETS_REQUESTED AS DOUBLE) AS PALLETS_REQUESTED,
	CAST(PKEQ24_CASES_REQUESTED AS DOUBLE) AS PKEQ24_CASES_REQUESTED,
	CAST(QTY_SHIPPED AS DOUBLE) AS QTY_SHIPPED,
	CAST(PALLETS_SHIPPED AS DOUBLE) AS PALLETS_SHIPPED,
	CAST(PKEQ24_CASES_SHIPPED AS DOUBLE) AS PKEQ24_CASES_SHIPPED,
	CAST(QTY_SHORT_SHIPPED AS DOUBLE) AS QTY_SHORT_SHIPPED,
	CAST(PALLETS_SHORT_SHIPPED AS DOUBLE) AS PALLETS_SHORT_SHIPPED,
	CAST(PKEQ24_CASES_SHORT_SHIPPED AS DOUBLE) AS PKEQ24_CASES_SHORT_SHIPPED,
	SHORT_SHIP_REASON_CODE,
	DATE_CHANGE_REASON_CODE,
	REQUEST_DATE_FROM AS REQUEST_DATE_FROM,
	REQUEST_DATE_TO AS REQUEST_DATE_TO,
	FINAL_REQUEST_DATE  AS FINAL_REQUEST_DATE,
	SK_MCAL_DT_ID,
	ATTRIBUTE11,
	ORIGINAL_REQUEST_DATE AS ORIGINAL_REQUEST_DATE,
	ORIGINAL_SCHEDULE_SHIP_DATE,
	CUSTOMER_AGREED_DATE_FROM AS CUSTOMER_AGREED_DATE_FROM,
	CUSTOMER_AGREED_DATE_TO  AS CUSTOMER_AGREED_DATE_TO,
	FINAL_CUSTOMER_AGREED_DATE AS FINAL_CUSTOMER_AGREED_DATE,
	CAST(SHIP_CONFIRM_DATE AS TIMESTAMP) AS SHIP_CONFIRM_DATE,
	PICKUP_APPOINTMENT  AS PICKUP_APPOINTMENT,
	DELIVERY_APPOINTMENT AS DELIVERY_APPOINTMENT,
	STATUS,
	CANCEL_REASON_CODE,
	USERNAME,
	HIST_CREATION_DATE AS HIST_CREATION_DATE,
	LOCAL_ARRIVAL_DATE_214 AS LOCAL_ARRIVAL_DATE_214,
	SHIP_TO_CITY,
	SHIP_TO_STATE,
	ZIP_CODE,
	PARTY_SITE_NUMBER,
	SALES_CHANNEL_CODE,
	DTS_FLAG,
	DTS_DC_CLASSIFICATION,
	CARRIER_NAME,
	CAST(ORDER_CREATION_DATE AS TIMESTAMP) AS ORDER_CREATION_DATE,
	SHIPMENT_GID,
	ORDER_HEADER_STATUS,
	BOTTLE_SIZE,
	PACKAGE_SIZE,
	WATER_TYPE,
	WATERTYPE,
	WATER_TYPE_SC,
	PRODUCT,
	PRODUCT_FAMILY,
	PRODUCT_CATEGORY,
	SALES_CATEGORY,
	REASON_COMMENTS,
    CASE 
		WHEN UPPER(ORDER_HEADER_STATUS) not in ('CANCELLED','CANCELED') AND UPPER(STATUS) not in ('CANCELLED','CANCELED') 
		THEN LOAD_TYPE
	END LOAD_TYPE,
	CASE 
		WHEN UPPER(ORDER_HEADER_STATUS) not in ('CANCELLED','CANCELED') AND UPPER(STATUS)not in ('CANCELLED','CANCELED')
		THEN LOAD_TYPE1
	END LOAD_TYPE1,
    TRANSPORT_MODE_GID,
    SHIPMENT_DROP,
	EARLY_DELIVERY_DATE  AS EARLY_DELIVERY_DATE,
	LATE_DELIVERY_DATE  AS LATE_DELIVERY_DATE,
	DELIVERY_WINDOW_START AS DELIVERY_WINDOW_START,
	DELIVERY_WINDOW_END AS DELIVERY_WINDOW_END,
	LATEST_CHANGE_FILTER,
	ITEM_CUSTOMER_WANT_DATE,
	QTY_CORRECT,
	LEAD_TIME_DAYS,
	CASE
		WHEN UPPER(TRIM(CUSTOMER)) LIKE '%ESSENTIA%' THEN
			CASE
				WHEN ITEM_CUSTOMER_WANT_DATE = '1. CWD' THEN
					CASE
						WHEN CAST(FINAL_CUSTOMER_DELIVERY_DT AS DATE) <= CAST(FINAL_CUSTOMER_AGREED_DATE AS DATE)
						THEN 0
						ELSE 1
					END
				ELSE DATEDIFF(
						CAST(FINAL_CUSTOMER_DELIVERY_DT AS DATE),
						CAST(FINAL_CUSTOMER_AGREED_DATE AS DATE)
					)
			END
		ELSE DATEDIFF(
					TO_DATE(FINAL_CUSTOMER_DELIVERY_DT, 'MM-dd-yyyy'),
					TO_DATE(FINAL_CUSTOMER_COMMITTED_DT, 'MM-dd-yyyy'))
	END AS RECOVERY_TIME_DAYS,
	CAST(ORDER_COUNTER AS DOUBLE) AS ORDER_COUNTER,
	ITEM_COUNTER,
	PUSH_PULL_FLAG,
	CAST(SHIPMENT_COUNTER AS double) AS SHIPMENT_COUNTER,
	CHANGE_COUNTER,
	--FINAL_CUSTOMER_AGREED_DATE_BNC,
	ROLLUP_ORGANIZATION_CODE,
	SUPPLY_CHAIN_REGION,
	DOMAIN_NAME,
	SHIP_TO_COUNTRY,
	CHANGE_TYPE,
	DEST_ORDER_CREATION_DATE AS DEST_ORDER_CREATION_DATE,
	DEST_PICKUP_APPOINTMENT AS DEST_PICKUP_APPOINTMENT,
	BUFFERED_APPOINTMENT_TIME AS BUFFERED_APPOINTMENT_TIME,
	ITEM_ON_TIME,
	CONTRACTUAL_LEAD_TIME,
	CS_TEAM,
	CUSTOMER_REGION,
	CUSTOMER_LOCATION_GID,
	--CAST(YEAR_NUMBER AS INT) AS YEAR_NUMBER,
    CAST(PROM_WEEK.MCAL_YEAR AS INT) AS YEAR_NUMBER,
	DEPLOYMENT_FLAG,
	CAST(ACTUAL_CASES_REQUESTED AS DOUBLE) AS ACTUAL_CASES_REQUESTED,
	CAST(ACTUAL_CASES_SHIPPED AS DOUBLE) AS ACTUAL_CASES_SHIPPED,
	CAST(ACTUAL_CASES_SHORT_SHIPPED AS DOUBLE) AS ACTUAL_CASES_SHORT_SHIPPED,
	SHIPPING_REGION,
	CS_REPS,
	OR_USER_DEFINED1_ICON_GID,
	STATUS_TYPE_GID,
	STATUS_VALUE_GID,
	NIAGARA_SALES_REGION,
	OR_RATE_OFFERING_GID,
	ORDER_RELEASE_INDICATOR,
	CAST(FINAL_REQUEST_YEAR AS INT) AS FINAL_REQUEST_YEAR,
	CAST(FINAL_REQUEST_WEEK AS INT) AS FINAL_REQUEST_WEEK,
	RATE_GEO_GID,
	X_LANE_GID,
	CAST(FINAL_MILES AS DOUBLE) AS FINAL_MILES,
	OR_PENDING_REASON,
	SCHEDULED_FLAG,
	SH_RATE_OFFERING_GID,
	SH_RATE_GEO_GID,
	CAST(CANCELLED_QUANTITY AS DOUBLE) AS CANCELLED_QUANTITY,
	CAST(CANCELLED_PALLETS AS DOUBLE) AS CANCELLED_PALLETS,
	CAST(CANCELLED_24PKEQCASES AS DOUBLE) AS CANCELLED_24PKEQCASES,
	SH_X_LANE_GID,
	SH_TRANSPORTATION_MODE_GID,
	SH_DEPLOYMENT_FLAG,
	SCHEDULE_INSERT_DATE AS SCHEDULE_INSERT_DATE,
	SCHEDULE_INSERT_DATE_PST  AS SCHEDULE_INSERT_DATE_PST,
	SCHEDULE_INSERT_DATE_LOCAL AS SCHEDULE_INSERT_DATE_LOCAL,
	SCHEDULE_UPDATE_DATE,
	SCHEDULE_UPDATE_DATE_PST,
	SCHEDULE_UPDATE_DATE_LOCAL,
	SCHEDULE_UPDATE_USER,
	CAST(ORIGINAL_REQUEST_YEAR AS INT) AS ORIGINAL_REQUEST_YEAR,
	CAST(ORIGINAL_REQUEST_WEEK AS INT) AS ORIGINAL_REQUEST_WEEK,
	ADDRESS,
	DOCK_DOOR,
	DOCK_DOOR_ALLOCATION AS DOCK_DOOR_ALLOCATION,
	CAST(STARTDATETIME AS TIMESTAMP) AS STARTDATETIME,
	FIRST_LOAD_TIME AS FIRST_LOAD_TIME,
	LAST_LOAD_TIME AS LAST_LOAD_TIME,
	CHECK_IN_TIME AS CHECK_IN_TIME,
	CHECK_OUT_TIME AS CHECK_OUT_TIME,
	CLOSE_TRAILER AS CLOSE_TRAILER,
	CLOSE_TRAILER_LOCAL AS CLOSE_TRAILER_LOCAL,
	FGBRAND,
	DEF_ORG,
	DEF_ROLLUP_ORG_CODE,
	ORDER_RELEASE_INSERT_DATE AS ORDER_RELEASE_INSERT_DATE,
	LOCAL_ACTUAL_DEPARTURE AS LOCAL_ACTUAL_DEPARTURE,
	SHIP_TO_ORG,
	SHIP_TO_ROLLUP_ORG,
	SHIP_TO_SC_REGION,
	ORDER_TYPE_CODE,
	FIRST_RESCHEDULE_REASON_CODE,
	LAST_RESCHEDULE_REASON_CODE,
	CRITICAL_FLAG,
	OSP,
	MOSP,
	ORIGINAL_REQUEST_DATE_PST AS ORIGINAL_REQUEST_DATE_PST,
	ATTRIBUTE1,
	ATTRIBUTE2,
	ATTRIBUTE3,
	ATTRIBUTE4,
	ATTRIBUTE5,
	ATTRIBUTE6,
	ATTRIBUTE7,
	ATTRIBUTE8,
	ATTRIBUTE9,
	ATTRIBUTE10,
	NIAGARA_CHANGE,
	BUFFERED_APP_TIME AS BUFFERED_APP_TIME,
	CAST(NEW_CANCELLED_QUANTITY AS DOUBLE) AS NEW_CANCELLED_QUANTITY,
	REQUEST_DATE AS REQUEST_DATE,
	CUSTOMER_AGRRED_DATE AS CUSTOMER_AGRRED_DATE,
	PRODUCT_CATEGORY_SC,
	BUFFER_TIME_MINUTES,
	SK_ITEM_ID,
	CAST(CANCELLED_CASES AS DOUBLE) AS CANCELLED_CASES,
	CHECK_IN_TIME_LOCAL AS CHECK_IN_TIME_LOCAL,
	CHECK_IN_TIME_PST,
	CHECK_OUT_TIME_LOCAL AS CHECK_OUT_TIME_LOCAL,
	CHECK_OUT_TIME_PST,
	OR_INSERT_DATE_PST AS OR_INSERT_DATE_PST,
	CAST(APPOINTMENT_PICKUP_PST AS TIMESTAMP) AS APPOINTMENT_PICKUP_PST,
	ORDERED_ITEM,
	APPOINTMENT_DELIVERY_PST  AS APPOINTMENT_DELIVERY_PST,
	DELIVERY_WINDOW_START_LOCAL AS DELIVERY_WINDOW_START_LOCAL,
	DELIVERY_WINDOW_END_LOCAL AS DELIVERY_WINDOW_END_LOCAL,
    CASE 
		WHEN UPPER(ORDER_HEADER_STATUS) not in ('CANCELLED','CANCELED') AND UPPER(STATUS)not in ('CANCELLED','CANCELED') 
		THEN SUPPLY_CHAIN_LOADTYPE
	END SUPPLY_CHAIN_LOADTYPE,
	CREATED_ON_DT_SRC_TZ AS CREATED_ON_DT_SRC_TZ,
	SHIP_CONFIRM_DATE_LOCAL  AS SHIP_CONFIRM_DATE_LOCAL,
	CUSTOMER_ITEM,
	FG_BUSINESS_UNIT,
	FG_FLAVOR,
	PRODUCT_TYPE,
	CPU_CONTRACT,
	DEMAND_CLASS_CODE,
	AOP_RESULT,
	GENPLAN_RESULT,
	SH_USER_DEFINED1_ICON_GID,
	SK_X_DESTINATION_ORGANIZATION_ID,
	SK_INVENTORY_ORG_ID,
	SK_X_UOM_ID,
	CAST(ORIGINAL_REQUESTED_QTY AS DOUBLE) AS ORIGINAL_REQUESTED_QTY,
	CAST(SOA_SHIPPED_QUANTITY AS DOUBLE) AS SOA_SHIPPED_QUANTITY,
	--ACTUAL_SOA_SHIPPED_QUANTITY,
	CAST(SOA_SHIPMENT_DATE AS TIMESTAMP) AS SOA_SHIPMENT_DATE,
	SOA_CHECK_IN_TIME AS SOA_CHECK_IN_TIME,
	SOA_CHECK_OUT_TIME AS SOA_CHECK_OUT_TIME,
	EQUIPMENT_NUMBER,
	SHIPMENT_PRELOAD,
	SK_X_SHIP_TO_ORG_ID,
	ADDRESS_DESCRIPTION,
	OREF_SCHEDULE_UPDATE_DATE AS OREF_SCHEDULE_UPDATE_DATE,
	OREF_SCHEDULE_INSERT_USER,
	OREF_SCHEDULE_UPDATE_DATE_LOCAL AS OREF_SCHEDULE_UPDATE_DATE_LOCAL,
	OREF_SCHEDULE_INSERT_DATE_LOCAL AS OREF_SCHEDULE_INSERT_DATE_LOCAL,
	OREF_SCHEDULE_UPDATE_DATE_PST AS OREF_SCHEDULE_UPDATE_DATE_PST,
	OREF_SCHEDULE_INSERT_DATE_PST AS OREF_SCHEDULE_INSERT_DATE_PST,
	WITH_DRAW_USER_FLAG,
	FINAL_REQUEST_DATE_PST AS FINAL_REQUEST_DATE_PST,
	EARLIEST_FINAL_CUST_AGREED_DT AS EARLIEST_FINAL_CUST_AGREED_DT,
	RUSH,
	FINAL_CUSTOMER_DELIVERY_DT AS FINAL_CUSTOMER_DELIVERY_DT,
	FINAL_CUSTOMER_COMMITTED_DT AS FINAL_CUSTOMER_COMMITTED_DT,
	MTO_FLG,
	CAST(STANDARD_LEAD_TIME AS STRING) AS STANDARD_LEAD_TIME,
    LEAD_TIME_ADHERENCE,
    CAST(ORIGINAL_REQUESTED_QTY_CASES AS INT) AS ORIGINAL_REQUESTED_QTY_CASES,
    CS_SCHEDULER,
    PUA_REASON_CODE, 
	EARLY_PICKUP_DATE AS EARLY_PICKUP_DATE,
	ORDERED_ON_DT AS ORDERED_ON_DT,
	OR_TRANSPORTATION_MODE_GID,
	-- Audit
	CAST(DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyyyMMddHHmmss') AS BIGINT) AS ETL_PROCESS_ID,
	'N' AS IS_DELETED_FLAG ,
	--$$FUSION_DATASOURCE_NUM AS SOURCE_APP_ID, 
	CURRENT_TIMESTAMP()  AS DW_LASTUPDATE_DATE
    FROM (
			SELECT  S_VIEW2.*
				,
                TO_DATE(FINAL_CUSTOMER_AGREED_DATE) AS FINAL_CUSTOMER_AGREED_TO_DATE,
                ------------------------------Modified column derivation by Ankit 02/23/2025------------------------------
				CASE
					WHEN FINAL_CUSTOMER_DELIVERY_DT IS NULL THEN '3. Late'
					ELSE
						CASE
							WHEN CUSTOMER LIKE '%ESSENTIA%' THEN
								CASE
									WHEN to_date(from_utc_timestamp(FINAL_CUSTOMER_DELIVERY_DT,'America/Los_Angeles')) > to_date(from_utc_timestamp(FINAL_CUSTOMER_COMMITTED_DT,'America/Los_Angeles'))
										THEN '3. Late'
									ELSE '1. CWD'
								END
							WHEN LOAD_TYPE = 'CustomerPickup' 
							OR CARRIER_NAME = 'WAL-MART TRANSPORTATION, INC.' 
							OR (CARRIER_NAME = 'HARALAMBOS BEVERAGE CO' AND CUSTOMER = 'TALKING RAIN BEVERAGE COMPANY, INC.') 
							THEN
								CASE
																	WHEN COALESCE(
											datediff(
												to_date(from_utc_timestamp(FINAL_CUSTOMER_DELIVERY_DT, 'America/Los_Angeles')),
												to_date(from_utc_timestamp(FINAL_CUSTOMER_COMMITTED_DT, 'America/Los_Angeles'))
											), 0
										) <= 0 
										THEN '1. CWD'

									WHEN COALESCE(
											datediff(
												to_date(from_utc_timestamp(FINAL_CUSTOMER_DELIVERY_DT, 'America/Los_Angeles')),
												to_date(from_utc_timestamp(FINAL_CUSTOMER_COMMITTED_DT, 'America/Los_Angeles'))
											), 0
										) = 1  
										THEN '2. CWD + 1'

									ELSE '3. Late'
								END
							ELSE
								CASE
																	WHEN COALESCE(
											datediff(
												to_date(from_utc_timestamp(FINAL_CUSTOMER_DELIVERY_DT, 'America/Los_Angeles')),
												to_date(from_utc_timestamp(FINAL_CUSTOMER_COMMITTED_DT, 'America/Los_Angeles'))
											), 0
										) <= 0 
										THEN '1. CWD'

									WHEN COALESCE(
											datediff(
												to_date(from_utc_timestamp(FINAL_CUSTOMER_DELIVERY_DT, 'America/Los_Angeles')),
												to_date(from_utc_timestamp(FINAL_CUSTOMER_COMMITTED_DT, 'America/Los_Angeles'))
											), 0
										) = 1  
										THEN '2. CWD + 1'

									ELSE '3. Late'
								END
						END
				END AS ITEM_CUSTOMER_WANT_DATE
  ------------------------------Modified column derivation by Ankit 02/23/2025------------------------------    
			FROM 
  (  SELECT
        S_VIEW1.HISTORY_TYPE                                                             HISTORY_TYPE,
        --PROM_WEEK.MCAL_WEEK                                                              WEEK_NUMBER,
        S_VIEW1.TRIP_ID                                                                  TRIP_ID,
        S_VIEW1.STOP_NUMBER                                                              STOP_NUMBER,
        S_VIEW1.CUSTOMER                                                                 CUSTOMER,
        S_VIEW1.CUST_PO                                                                  CUST_PO,
        S_VIEW1.ORDER_NUMBER                                                             ORDER_NUMBER,
        S_VIEW1.LINE_NUMBER                                                              LINE_NUMBER,
        S_VIEW1.DELIVERY_NUMBER                                                          DELIVERY_NUMBER,
        S_VIEW1.FREIGHT_METHOD                                                           FREIGHT_METHOD,
        S_VIEW1.ORGANIZATION_CODE                                                        ORGANIZATION_CODE,
        S_VIEW1.ITEM                                                                     ITEM,
        S_VIEW1.UOM                                                                      UOM,
        CASE
            WHEN S_VIEW1.LATEST_CHANGE_FILTER = 'Y' THEN
                S_VIEW1.QTY_REQUESTED
            ELSE 0
        END QTY_REQUESTED,
        CASE
            WHEN S_VIEW1.LATEST_CHANGE_FILTER = 'Y' THEN
                S_VIEW1.ACTUAL_PALLETS_REQUESTED
            ELSE 0
        END PALLETS_REQUESTED,
        CASE
            WHEN S_VIEW1.LATEST_CHANGE_FILTER = 'Y' THEN
                S_VIEW1.CASES_24PKEQ_REQUESTED
            ELSE 0
        END PKEQ24_CASES_REQUESTED,
        CASE
            WHEN S_VIEW1.LATEST_CHANGE_FILTER = 'Y' THEN
                S_VIEW1.QTY_SHIPPED
            ELSE 0
        END QTY_SHIPPED,
        CASE
            WHEN S_VIEW1.LATEST_CHANGE_FILTER = 'Y' THEN
                S_VIEW1.ACTUAL_PALLETS_SHIPPED
            ELSE 0
        END PALLETS_SHIPPED,
        CASE
            WHEN S_VIEW1.LATEST_CHANGE_FILTER = 'Y' THEN
                S_VIEW1.CASES_24PKEQ_SHIPPED
            ELSE 0
        END PKEQ24_CASES_SHIPPED,
        CASE
            WHEN S_VIEW1.LATEST_CHANGE_FILTER = 'Y' THEN
                S_VIEW1.QTY_SHORT_SHIPPED
            ELSE 0
        END QTY_SHORT_SHIPPED,
        CASE
            WHEN S_VIEW1.LATEST_CHANGE_FILTER = 'Y' THEN
                S_VIEW1.ACTUAL_PALLETS_SHORT_SHIPPED
            ELSE 0
        END PALLETS_SHORT_SHIPPED,
        CASE
            WHEN S_VIEW1.LATEST_CHANGE_FILTER = 'Y' THEN
                S_VIEW1.CASES_24PKEQ_SHORT_SHIPPED
            ELSE 0
        END PKEQ24_CASES_SHORT_SHIPPED,
        NVL(S_VIEW1.SHORT_SHIP_REASON_CODE, '-')                                         SHORT_SHIP_REASON_CODE,
        S_VIEW1.DATE_CHANGE_REASON_CODE                                                  DATE_CHANGE_REASON_CODE,
        S_VIEW1.REQUEST_DATE_FROM                                                        REQUEST_DATE_FROM,
        S_VIEW1.REQUEST_DATE_TO                                                          REQUEST_DATE_TO,
        S_VIEW1.FINAL_REQUEST_DATE                                                       FINAL_REQUEST_DATE,
		CAST(DATE_FORMAT(S_VIEW1.FINAL_REQUEST_DATE,'yyyyMMdd') AS INT) 				 SK_MCAL_DT_ID,
        S_VIEW1.ATTRIBUTE11                                                              ATTRIBUTE11,
        S_VIEW1.ORIGINAL_REQUEST_DATE                                                    ORIGINAL_REQUEST_DATE,
        S_VIEW1.ORIGINAL_SCHEDULE_SHIP_DATE                                              ORIGINAL_SCHEDULE_SHIP_DATE,
        S_VIEW1.CUSTOMER_AGREED_DATE_FROM                                                CUSTOMER_AGREED_DATE_FROM,
        S_VIEW1.CUSTOMER_AGREED_DATE_TO                                                  CUSTOMER_AGREED_DATE_TO,
        S_VIEW1.FINAL_CUSTOMER_AGREED_DATE                                               FINAL_CUSTOMER_AGREED_DATE,
        S_VIEW1.SHIP_CONFIRM_DATE                                                        SHIP_CONFIRM_DATE,
        S_VIEW1.PICKUP_APPOINTMENT                                                       PICKUP_APPOINTMENT,
        S_VIEW1.DELIVERY_APPOINTMENT                                                     DELIVERY_APPOINTMENT,
        UPPER(S_VIEW1.STATUS)                                                            STATUS,
        S_VIEW1.CANCEL_REASON_CODE                                                       CANCEL_REASON_CODE,
        S_VIEW1.USERNAME                                                                 USERNAME,
        S_VIEW1.HIST_CREATION_DATE                                                       HIST_CREATION_DATE,
        S_VIEW1.LOCAL_ARRIVAL_DATE_214                                                   LOCAL_ARRIVAL_DATE_214,
        S_VIEW1.SHIP_TO_CITY                                                             SHIP_TO_CITY,
        S_VIEW1.SHIP_TO_STATE                                                            SHIP_TO_STATE,
        S_VIEW1.ZIP_CODE                                                                 ZIP_CODE,
        S_VIEW1.PARTY_SITE_NUMBER                                                        PARTY_SITE_NUMBER,
        S_VIEW1.SALES_CHANNEL_CODE                                                       SALES_CHANNEL_CODE,
        S_VIEW1.DTS_FLAG                                                                 DTS_FLAG,
        S_VIEW1.DTS_DC_CLASSIFICATION                                                    DTS_DC_CLASSIFICATION,
        S_VIEW1.CARRIER_NAME                                                             CARRIER_NAME,
        S_VIEW1.ORDER_CREATION_DATE                                                      ORDER_CREATION_DATE,
        S_VIEW1.SHIPMENT_GID                                                             SHIPMENT_GID,
        S_VIEW1.ORDER_HEADER_STATUS                                                      ORDER_HEADER_STATUS,
        S_VIEW1.BOTTLE_SIZE                                                              BOTTLE_SIZE,
        S_VIEW1.PACKAGE_SIZE                                                             PACKAGE_SIZE,
        S_VIEW1.WATER_TYPE                                                               WATER_TYPE,
		S_VIEW1.WATERTYPE                                                                WATERTYPE,
        S_VIEW1.WATER_TYPE_SC                                                            WATER_TYPE_SC,
        S_VIEW1.PRODUCT                                                                  PRODUCT,
        S_VIEW1.PRODUCT_FAMILY                                                           PRODUCT_FAMILY,
        S_VIEW1.PRODUCT_CATEGORY                                                         PRODUCT_CATEGORY,
        S_VIEW1.SALES_CATEGORY                                                           SALES_CATEGORY,
        NVL(S_VIEW1.REASON_COMMENTS, '-')                                                REASON_COMMENTS,
        S_VIEW1.LOAD_TYPE                                                                LOAD_TYPE, 
        S_VIEW1.LOAD_TYPE                                                                LOAD_TYPE1,
        S_VIEW1.TRANSPORT_MODE_GID                                                       TRANSPORT_MODE_GID,
        S_VIEW1.SHIPMENT_DROP                                                            SHIPMENT_DROP,
        S_VIEW1.EARLY_DELIVERY_DATE                                                      EARLY_DELIVERY_DATE,
        S_VIEW1.LATE_DELIVERY_DATE                                                       LATE_DELIVERY_DATE,
        S_VIEW1.DELIVERY_WINDOW_START                                                    DELIVERY_WINDOW_START,
        S_VIEW1.DELIVERY_WINDOW_END                                                      DELIVERY_WINDOW_END,
        S_VIEW1.LATEST_CHANGE_FILTER                                                     LATEST_CHANGE_FILTER,
        S_VIEW1.QTY_CORRECT                                                              QTY_CORRECT,
        S_VIEW1.LEAD_TIME_DAYS                                                           LEAD_TIME_DAYS,
        S_VIEW1.ORDER_COUNTER                                                            ORDER_COUNTER,
        S_VIEW1.ITEM_COUNTER                                                             ITEM_COUNTER,
        CASE
            WHEN S_VIEW1.HISTORY_TYPE = 'UPDATE' THEN
                    CASE
                        WHEN TO_DATE(S_VIEW1.CUSTOMER_AGREED_DATE_TO) > TO_DATE(S_VIEW1.CUSTOMER_AGREED_DATE_FROM) THEN
                            'Push'
                        WHEN TO_DATE(S_VIEW1.CUSTOMER_AGREED_DATE_TO) < TO_DATE(S_VIEW1.CUSTOMER_AGREED_DATE_FROM) THEN
                            'Pull'
                        ELSE NULL
                    END
            ELSE NULL
        END                                                                              PUSH_PULL_FLAG,
        S_VIEW1.SHIPMENT_COUNTER                                                         SHIPMENT_COUNTER,
        S_VIEW1.CHANGE_COUNTER                                                           CHANGE_COUNTER,
        CAST(NULL AS STRING)                                                             FINAL_CUSTOMER_AGREED_DATE_BNC,
        S_VIEW1.ROLLUP_ORGANIZATION_CODE                                                 ROLLUP_ORGANIZATION_CODE,
        S_VIEW1.SUPPLY_CHAIN_REGION                                                      SUPPLY_CHAIN_REGION,
        S_VIEW1.DOMAIN_NAME                                                              DOMAIN_NAME,
        S_VIEW1.SHIP_TO_COUNTRY                                                          SHIP_TO_COUNTRY,
		CASE
				WHEN S_VIEW1.HISTORY_TYPE = 'UPDATE' THEN
					CASE
						WHEN TO_DATE(from_utc_timestamp(S_VIEW1.CUSTOMER_AGREED_DATE_TO, 'America/Los_Angeles')) 
							<> 
							TO_DATE(from_utc_timestamp(S_VIEW1.CUSTOMER_AGREED_DATE_FROM, 'America/Los_Angeles')) THEN
							'Customer Change'

						WHEN TO_DATE(from_utc_timestamp(S_VIEW1.REQUEST_DATE_TO, 'America/Los_Angeles')) 
							<> 
							TO_DATE(from_utc_timestamp(S_VIEW1.REQUEST_DATE_FROM, 'America/Los_Angeles')) THEN
							'Niagara Change'

						ELSE
							'Other'
					END
				ELSE NULL
		END AS CHANGE_TYPE, -- MODIFIED BY ANKIT: BUG NO. 144843
        S_VIEW1.DEST_ORDER_CREATION_DATE                                               	DEST_ORDER_CREATION_DATE,
        S_VIEW1.DEST_PICKUP_APPOINTMENT                                                	DEST_PICKUP_APPOINTMENT,
        CASE
            WHEN S_VIEW1.LOAD_TYPE = 'CustomerPickup'
                OR S_VIEW1.CARRIER_NAME = 'WAL-MART TRANSPORTATION, INC.'
                OR ( S_VIEW1.CARRIER_NAME = 'HARALAMBOS BEVERAGE CO'
               AND S_VIEW1.CUSTOMER = 'TALKING RAIN BEVERAGE COMPANY, INC.' ) THEN
                S_VIEW1.FINAL_REQUEST_DATE
            WHEN S_VIEW1.DELIVERY_WINDOW_END IS NOT NULL
               AND ( S_VIEW1.DELIVERY_WINDOW_END > S_VIEW1.DELIVERY_APPOINTMENT
                OR S_VIEW1.DELIVERY_APPOINTMENT IS NULL ) THEN
                S_VIEW1.DELIVERY_WINDOW_END
            WHEN S_VIEW1.DELIVERY_APPOINTMENT IS NOT NULL
               AND S_VIEW1.BUFFER_TIME_MINUTES IS NOT NULL
                THEN TIMESTAMPADD(MINUTE,S_VIEW1.BUFFER_TIME_MINUTES ,S_VIEW1.DELIVERY_APPOINTMENT) 
            WHEN S_VIEW1.DELIVERY_APPOINTMENT IS NOT NULL THEN
                TIMESTAMPADD(HOUR,1,S_VIEW1.DELIVERY_APPOINTMENT)
            ELSE NULL
        END BUFFERED_APPOINTMENT_TIME,
        CASE
            WHEN S_VIEW1.LOAD_TYPE = 'CustomerPickup'
                OR S_VIEW1.CARRIER_NAME = 'WAL-MART TRANSPORTATION, INC.' THEN
                    CASE
                        WHEN TO_DATE(S_VIEW1.FINAL_REQUEST_DATE) > TO_DATE(S_VIEW1.FINAL_CUSTOMER_AGREED_DATE) THEN
                            '2. Late'
                        WHEN ( S_VIEW1.DEST_PICKUP_APPOINTMENT ) <= ( S_VIEW1.FINAL_REQUEST_DATE )         THEN
                            '1. On-Time'
                        ELSE
                            '2. Late'
                    END
            WHEN S_VIEW1.SHIPMENT_DROP = 'Drop'                                                    THEN
                    CASE
                        WHEN S_VIEW1.LOCAL_ARRIVAL_DATE_214 IS NULL
                           AND S_VIEW1.DELIVERY_APPOINTMENT IS NULL THEN
                            '2. Late'
                        WHEN S_VIEW1.LOCAL_ARRIVAL_DATE_214 IS NULL
                           AND TO_DATE(S_VIEW1.DELIVERY_APPOINTMENT) > TO_DATE(S_VIEW1.FINAL_CUSTOMER_AGREED_DATE) THEN
                            '2. Late'
                        WHEN S_VIEW1.LOCAL_ARRIVAL_DATE_214 IS NULL THEN
                            '3. EDI 214 is missing'
                        WHEN TO_DATE(S_VIEW1.LOCAL_ARRIVAL_DATE_214) <= TO_DATE(S_VIEW1.FINAL_CUSTOMER_AGREED_DATE) THEN
                            '1. On-Time'
                        ELSE
                            '2. Late'
                    END
            WHEN S_VIEW1.LOCAL_ARRIVAL_DATE_214 IS NULL
               AND S_VIEW1.DELIVERY_APPOINTMENT IS NULL THEN
                '2. Late'
            WHEN LOCAL_ARRIVAL_DATE_214 IS NULL
               AND TO_DATE(S_VIEW1.DELIVERY_APPOINTMENT) > TO_DATE(S_VIEW1.FINAL_CUSTOMER_AGREED_DATE) THEN
                '2. Late'
            WHEN S_VIEW1.LOCAL_ARRIVAL_DATE_214 IS NULL THEN
                '3. EDI 214 is missing'
            WHEN TO_DATE(S_VIEW1.DELIVERY_APPOINTMENT) > TO_DATE(S_VIEW1.FINAL_CUSTOMER_AGREED_DATE) THEN
                '2. Late'
            WHEN LOCAL_ARRIVAL_DATE_214 < (
												CASE
													WHEN S_VIEW1.LOAD_TYPE = 'CustomerPickup'
														OR S_VIEW1.CARRIER_NAME = 'WAL-MART TRANSPORTATION, INC.'
														OR ( S_VIEW1.CARRIER_NAME = 'HARALAMBOS BEVERAGE CO'
													AND S_VIEW1.CUSTOMER = 'TALKING RAIN BEVERAGE COMPANY, INC.' ) THEN
														S_VIEW1.FINAL_REQUEST_DATE
													WHEN S_VIEW1.DELIVERY_WINDOW_END IS NOT NULL
													AND ( S_VIEW1.DELIVERY_WINDOW_END > S_VIEW1.DELIVERY_APPOINTMENT
														OR S_VIEW1.DELIVERY_APPOINTMENT IS NULL ) THEN
														S_VIEW1.DELIVERY_WINDOW_END
													WHEN S_VIEW1.DELIVERY_APPOINTMENT IS NOT NULL
													AND S_VIEW1.BUFFER_TIME_MINUTES IS NOT NULL THEN
														 TIMESTAMPADD(MINUTE, S_VIEW1.BUFFER_TIME_MINUTES ,S_VIEW1.DELIVERY_APPOINTMENT)
													WHEN S_VIEW1.DELIVERY_APPOINTMENT IS NOT NULL THEN
														TIMESTAMPADD(HOUR,1,S_VIEW1.DELIVERY_APPOINTMENT)
													ELSE
														NULL
												END
											) THEN '1. On-Time'
            ELSE '2. Late'
        END ITEM_ON_TIME, 
        S_VIEW1.CONTRACTUAL_LEAD_TIME                                                    CONTRACTUAL_LEAD_TIME,
        S_VIEW1.CS_TEAM                                                                  CS_TEAM,
        S_VIEW1.CUSTOMER_REGION                                                          CUSTOMER_REGION,
        S_VIEW1.CUSTOMER_LOCATION_GID                                                    CUSTOMER_LOCATION_GID,
        --PROM_WEEK.MCAL_YEAR                                                              YEAR_NUMBER, 
        S_VIEW1.DEPLOYMENT_FLAG                                                          DEPLOYMENT_FLAG,
        CASE
            WHEN S_VIEW1.LATEST_CHANGE_FILTER = 'Y' THEN
                S_VIEW1.ACTUAL_CASES_REQUESTED
            ELSE 0
        END ACTUAL_CASES_REQUESTED,
        CASE
            WHEN S_VIEW1.LATEST_CHANGE_FILTER = 'Y' THEN
                S_VIEW1.ACTUAL_CASES_SHIPPED
            ELSE 0
        END ACTUAL_CASES_SHIPPED,
        CASE
            WHEN S_VIEW1.LATEST_CHANGE_FILTER = 'Y' THEN
                S_VIEW1.ACTUAL_CASES_SHORT_SHIPPED
            ELSE 0
        END ACTUAL_CASES_SHORT_SHIPPED,
        S_VIEW1.SHIPPING_REGION                                                          SHIPPING_REGION,
        S_VIEW1.CS_REPS                                                                  CS_REPS,
        S_VIEW1.OR_USER_DEFINED1_ICON_GID                                                OR_USER_DEFINED1_ICON_GID,
        S_VIEW1.STATUS_TYPE_GID,
        S_VIEW1.STATUS_VALUE_GID,
        S_VIEW1.NIAGARA_SALES_REGION,
        S_VIEW1.OR_RATE_OFFERING_GID,
        S_VIEW1.ORDER_RELEASE_INDICATOR,
        S_VIEW1.REQUEST_DT_YEAR                                                          FINAL_REQUEST_YEAR,
        S_VIEW1.REQUEST_DT_WEEK                                                          FINAL_REQUEST_WEEK,
        S_VIEW1.RATE_GEO_GID,
        S_VIEW1.X_LANE_GID,
        S_VIEW1.FINAL_MILES                                                              AS FINAL_MILES,
        NVL(S_VIEW1.OR_PENDING_REASON, '-')                                              AS OR_PENDING_REASON,
        S_VIEW1.SCHEDULED_FLAG,
        S_VIEW1.SH_RATE_OFFERING_GID,
        S_VIEW1.SH_RATE_GEO_GID,
        CASE
            WHEN S_VIEW1.LATEST_CHANGE_FILTER = 'Y' THEN
                S_VIEW1.CANCELLED_QUANTITY
            ELSE 0
        END CANCELLED_QUANTITY,
        CASE
            WHEN S_VIEW1.LATEST_CHANGE_FILTER = 'Y' THEN
                S_VIEW1.CANCELLED_PALLETS
            ELSE 0
        END CANCELLED_PALLETS,
        CASE
            WHEN S_VIEW1.LATEST_CHANGE_FILTER = 'Y' THEN
                S_VIEW1.CANCELLED_24PKEQCASES
            ELSE 0
        END CANCELLED_24PKEQCASES,
        S_VIEW1.SH_X_LANE_GID,
        S_VIEW1.SH_TRANSPORT_MODE_GID SH_TRANSPORTATION_MODE_GID,
        S_VIEW1.SH_DEPLOYMENT_FLAG,
        S_VIEW1.SCHEDULE_INSERT_DATE,
        S_VIEW1.SCHEDULE_INSERT_DATE_PST,
        S_VIEW1.SCHEDULE_INSERT_DATE_LOCAL,
        S_VIEW1.SCHEDULE_UPDATE_DATE,
        S_VIEW1.SCHEDULE_UPDATE_DATE_PST,
        S_VIEW1.SCHEDULE_UPDATE_DATE_LOCAL,
        S_VIEW1.SCHEDULE_UPDATE_USER,
        S_VIEW1.ADDRESS,
        S_VIEW1.DOCK_DOOR,
        S_VIEW1.DOCK_DOOR_ALLOCATION,
        S_VIEW1.STARTDATETIME,
        S_VIEW1.FIRST_LOAD_TIME,
        S_VIEW1.LAST_LOAD_TIME,
        S_VIEW1.CHECK_IN_TIME,
        S_VIEW1.CHECK_OUT_TIME,
        S_VIEW1.CLOSE_TRAILER,
		S_VIEW1.CLOSE_TRAILER_LOCAL,
        S_VIEW1.FG_BRAND FGBRAND,
        S_VIEW1.DEF_ORG DEF_ORG,
        S_VIEW1.DEF_ROLLUP_ORG_CODE,
        S_VIEW1.ORDER_RELEASE_INSERT_DATE,
        S_VIEW1.LOCAL_ACTUAL_DEPARTURE,
        S_VIEW1.SHIP_TO_ORG,
        S_VIEW1.SHIP_TO_ROLLUP_ORG,
        S_VIEW1.SHIP_TO_SC_REGION,
        S_VIEW1.ORDER_TYPE_CODE,
        S_VIEW1.FIRST_RESCHEDULE_REASON_CODE,
        S_VIEW1.CHANGE_REASON_CODE LAST_RESCHEDULE_REASON_CODE,
        S_VIEW1.X_SHIPMENT_PRIORITY_CODE CRITICAL_FLAG,
        S_VIEW1.OSP,
        S_VIEW1.MOSP,
        S_VIEW1.ORIGINAL_REQUEST_DATE_PST ORIGINAL_REQUEST_DATE_PST,
        CAST(NULL AS STRING)                                                             ATTRIBUTE1,
        CAST(NULL AS STRING)                                                             ATTRIBUTE2,
        CAST(NULL AS STRING)                                                             ATTRIBUTE3,
        CAST(NULL AS STRING)                                                             ATTRIBUTE4,
        CAST(NULL AS STRING)                                                             ATTRIBUTE5,
        CAST(NULL AS STRING)                                                             ATTRIBUTE6,
        CAST(NULL AS STRING)                                                             ATTRIBUTE7,
        CAST(NULL AS STRING)                                                             ATTRIBUTE8,
        CAST(NULL AS STRING)                                                             ATTRIBUTE9,
        CAST(NULL AS STRING)                                                             ATTRIBUTE10,
        S_VIEW1.NIAGARA_CHANGE                                                           NIAGARA_CHANGE,
        S_VIEW1.BUFFERED_APPOINTMENT_TIME                                                BUFFERED_APP_TIME,
        CASE
            WHEN LATEST_CHANGE_FILTER = 'Y' THEN
                S_VIEW1.CANCELLED_QUANTITY
            ELSE 0
        END NEW_CANCELLED_QUANTITY,
        S_VIEW1.REQUEST_DATE                                                             REQUEST_DATE,
        S_VIEW1.CUSTOMER_AGRRED_DATE                                                     CUSTOMER_AGRRED_DATE,
        S_VIEW1.PRODUCT_CATEGORY_SC                                                      PRODUCT_CATEGORY_SC,
        S_VIEW1.BUFFER_TIME_MINUTES,
		SUBSTR(ORIGINAL_REQUEST_YEAR_WEEK, 1, 4) ORIGINAL_REQUEST_YEAR,
		SUBSTR(ORIGINAL_REQUEST_YEAR_WEEK, INSTR(ORIGINAL_REQUEST_YEAR_WEEK, '-') + 1) ORIGINAL_REQUEST_WEEK,
        S_VIEW1.SK_ITEM_ID,
        S_VIEW1.CANCELLED_CASES,
        S_VIEW1.CHECK_IN_TIME_LOCAL CHECK_IN_TIME_LOCAL,  
        CURRENT_DATE CHECK_IN_TIME_PST,    
        S_VIEW1.CHECK_OUT_TIME_LOCAL CHECK_OUT_TIME_LOCAL,
        CURRENT_DATE CHECK_OUT_TIME_PST,   
        S_VIEW1.OR_INSERT_DATE_PST,
        S_VIEW1.APPOINTMENT_PICKUP_PST,
        S_VIEW1.X_ORDERED_ITEM                                                             ORDERED_ITEM,
        S_VIEW1.APPOINTMENT_DELIVERY_PST,
        S_VIEW1.DELIVERY_WINDOW_START_LOCAL,
        S_VIEW1.DELIVERY_WINDOW_END_LOCAL,
        S_VIEW1.SUPPLY_CHAIN_LOADTYPE,
        S_VIEW1.X_CREATED_ON_DT_SRC_TZ                                                     CREATED_ON_DT_SRC_TZ,
        S_VIEW1.SHIP_CONFIRM_DATE_LOCAL,
        S_VIEW1.CUSTOMER_ITEM,
        S_VIEW1.FG_BUSINESS_UNIT,
        S_VIEW1.FG_FLAVOR,
        S_VIEW1.PRODUCT_TYPE,
        CAST (NULL AS STRING) CPU_CONTRACT,
        S_VIEW1.DEMAND_CLASS_CODE,
        S_VIEW1.AOP_RESULT,
        S_VIEW1.GENPLAN_RESULT,
        S_VIEW1.SH_USER_DEFINED1_ICON_GID,
        S_VIEW1.SK_X_DESTINATION_ORGANIZATION_ID,
        S_VIEW1.SK_INVENTORY_ORG_ID,
        S_VIEW1.SK_X_UOM_ID,
        CASE
            WHEN LATEST_CHANGE_FILTER = 'Y' THEN
                S_VIEW1.ORIGINAL_REQUESTED_QTY
            ELSE 0
        END ORIGINAL_REQUESTED_QTY,
        CASE
            WHEN LATEST_CHANGE_FILTER = 'Y' THEN
                S_VIEW1.SOA_SHIPPED_QUANTITY
            ELSE 0
        END SOA_SHIPPED_QUANTITY,
		 CASE
            WHEN LATEST_CHANGE_FILTER = 'Y' THEN
                S_VIEW1.ACTUAL_SOA_SHIPPED_QUANTITY
            ELSE 0
        END ACTUAL_SOA_SHIPPED_QUANTITY,
        S_VIEW1.SOA_SHIPMENT_DATE,
        S_VIEW1.SOA_CHECK_IN_TIME,
        S_VIEW1.SOA_CHECK_OUT_TIME,
        S_VIEW1.EQUIPMENT_NUMBER,
        S_VIEW1.SHIPMENT_PRELOAD,
        S_VIEW1.SK_X_SHIP_TO_ORG_ID,
        S_VIEW1.ADDRESS_DESCRIPTION,
        S_VIEW1.OREF_SCHEDULE_UPDATE_DATE,
        S_VIEW1.OREF_SCHEDULE_INSERT_USER,
        S_VIEW1.OREF_SCHEDULE_UPDATE_DATE_LOCAL,
        S_VIEW1.OREF_SCHEDULE_INSERT_DATE_LOCAL,
        S_VIEW1.OREF_SCHEDULE_UPDATE_DATE_PST,
        S_VIEW1.OREF_SCHEDULE_INSERT_DATE_PST,
        S_VIEW1.WITH_DRAW_USER_FLAG,
        S_VIEW1.FINAL_REQUEST_DATE_PST,
        MIN(
            CASE
                WHEN LATEST_CHANGE_FILTER = 'Y' THEN S_VIEW1.FINAL_CUSTOMER_AGREED_DATE
                ELSE NULL
            END
        ) OVER(PARTITION BY S_VIEW1.ORDER_NUMBER) EARLIEST_FINAL_CUST_AGREED_DT,
        CASE
            WHEN S_VIEW1.CONTRACTUAL_LEAD_TIME < 4
               AND S_VIEW1.LEAD_TIME_DAYS < S_VIEW1.CONTRACTUAL_LEAD_TIME THEN '1. Rush'
            WHEN S_VIEW1.CONTRACTUAL_LEAD_TIME >= 4
               AND S_VIEW1.LEAD_TIME_DAYS < 4 THEN '1. Rush'
            ELSE '2. Not Rush'
        END RUSH, 
        S_VIEW1.FINAL_CUSTOMER_DELIVERY_DATE                                             FINAL_CUSTOMER_DELIVERY_DT,  
        S_VIEW1.MTO_FLG                                                                  MTO_FLG,
        S_VIEW1.STANDARD_LEAD_TIME                                                       STANDARD_LEAD_TIME,                    
        CASE
            WHEN LEAD_TIME_DAYS >= STANDARD_LEAD_TIME THEN '2. Adhere'
            ELSE '1. Not Adhere'
        END LEAD_TIME_ADHERENCE,
        CASE WHEN LATEST_CHANGE_FILTER = 'Y' THEN S_VIEW1.ORIGINAL_REQUESTED_QTY_CASES ELSE 0 END ORIGINAL_REQUESTED_QTY_CASES,
		CASE
			WHEN CUSTOMER LIKE '%ESSENTIA%' THEN
				CASE
					WHEN dayofweek(FINAL_CUSTOMER_AGREED_DATE) IN (6, 7, 1) THEN
						CASE
							WHEN dayofweek(FINAL_CUSTOMER_AGREED_DATE) = 1 THEN date_add(FINAL_CUSTOMER_AGREED_DATE, 1)
							WHEN dayofweek(FINAL_CUSTOMER_AGREED_DATE) = 6 THEN date_add(FINAL_CUSTOMER_AGREED_DATE, 3)
							WHEN dayofweek(FINAL_CUSTOMER_AGREED_DATE) = 7 THEN date_add(FINAL_CUSTOMER_AGREED_DATE, 2)
						END
					ELSE date_add(FINAL_CUSTOMER_AGREED_DATE, 1)
				END
			WHEN LOAD_TYPE = 'CustomerPickup'
				OR CARRIER_NAME = 'WAL-MART TRANSPORTATION, INC.'
				OR (CARRIER_NAME = 'HARALAMBOS BEVERAGE CO'
					AND CUSTOMER = 'TALKING RAIN BEVERAGE COMPANY, INC.') THEN FINAL_CUSTOMER_AGREED_DATE
			ELSE FINAL_CUSTOMER_AGREED_DATE
		END AS FINAL_CUSTOMER_COMMITTED_DT,
        S_VIEW1.CS_SCHEDULER, 
        S_VIEW1.PUA_REASON_CODE,  
        S_VIEW1.EARLY_PICKUP_DATE,
		S_VIEW1.ORDERED_ON_DT,
		S_VIEW1.OR_TRANSPORTATION_MODE_GID
      FROM
        (
        SELECT
            S_VIEW.TRIP_ID,
			CONCAT(PLW.MCAL_YEAR, '-', PLW.MCAL_WEEK) AS ORIGINAL_REQUEST_YEAR_WEEK, 
            S_VIEW.STOP_NUMBER,
            S_VIEW.CUSTOMER,
            S_VIEW.CUST_PO,
            S_VIEW.ORDER_NUMBER,
            S_VIEW.LINE_NUMBER,
            S_VIEW.DELIVERY_NUMBER,
            S_VIEW.FREIGHT_METHOD,
            S_VIEW.ORGANIZATION_CODE,
            S_VIEW.ITEM,
            S_VIEW.DEPLOYMENT_FLAG,
            S_VIEW.UOM,
            S_VIEW.QTY_REQUESTED,
            S_VIEW.CASES_24PKEQ_REQUESTED,
            S_VIEW.ACTUAL_CASES_REQUESTED,
            S_VIEW.ACTUAL_PALLETS_REQUESTED,
            S_VIEW.QTY_SHIPPED,
            S_VIEW.CASES_24PKEQ_SHIPPED,
            S_VIEW.ACTUAL_CASES_SHIPPED,
            S_VIEW.ACTUAL_PALLETS_SHIPPED,
            S_VIEW.QTY_SHORT_SHIPPED,
            S_VIEW.CASES_24PKEQ_SHORT_SHIPPED,
            S_VIEW.ACTUAL_CASES_SHORT_SHIPPED,
            S_VIEW.ACTUAL_PALLETS_SHORT_SHIPPED,
            S_VIEW.SHORT_SHIP_REASON_CODE,
            S_VIEW.DATE_CHANGE_REASON_CODE,
            S_VIEW.REQUEST_DATE,
            CAST(NULL AS STRING)                        REQUEST_DATE_FROM_TEMP,
            CAST(NULL AS STRING)                        REQUEST_DATE_TO_TEMP,
            S_VIEW.FINAL_REQUEST_DATE,
            S_VIEW.ATTRIBUTE11,
            CAST(NULL AS STRING)                        ORIGINAL_REQUEST_DATE_TEMP,
            S_VIEW.ORIGINAL_SCHEDULE_SHIP_DATE,
            S_VIEW.CUSTOMER_AGRRED_DATE,
            CAST(NULL AS STRING)                        CUSTOMER_AGREED_DATE_FROM_TEMP,
            CAST(NULL AS STRING)        				CUSTOMER_AGREED_DATE_TEMP,
            CASE
                WHEN S_VIEW.CUSTOMER = 'COCA-COLA NAOU' THEN 
                        CASE
                            WHEN FIRST_VALUE(CHANGE_REASON_CODE, TRUE)
                                 OVER(PARTITION BY S_VIEW.ORDER_NUMBER
          
                                      ORDER BY
                                         CASE
                                             WHEN CHANGE_REASON_CODE IS NULL THEN
                                                 1
                                             ELSE
                                                 0
                                         END, HIST_CREATION_DATE ASC 
                                         ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                                 ) IN ( '01-CUSTOMER-CUSTOMER'
										,'02-CUSTOMER-CPU CARRIER'
										,'08-NIAGARA-WEATHER CONDITIONS'
										,'18-CUSTOMER-VOLUME SMOOTHING'
										,'27-CUSTOMER-FORECAST VARIATION'
										,'37-CUSTOMER-SUPPLIED RAWS'
										,'49-APPROVED LATE ORDER'
										,'50-PENDING CUSTOMER RESPONSE'
										,'51-APROVED PUSH ORDER'
										,'52-CUST REQUEST EARLY DATE'
										,'52-CUST REQUESTED EARLY DATE'
										,'53-CUST RECEIVING CONSTRAINT'
										,'55-CUST RESCHEDULED FOR FUTURE'
          								,'02-Customer-CPU Carrier',
										'08-Niagara-Weather Conditions',
										'18-Customer-Volume Smoothing',
										'49-Approved LATE ORDER',
										'52-Cust Requested Early Date',
										'53-Cust Receiving Constraint',
										'55-Cust Rescheduled for Future') THEN
                                FINAL_REQUEST_DATE
                            ELSE
                                ORIGINAL_REQUEST_DATE
                        END
                ELSE
                    S_VIEW.FINAL_CUSTOMER_AGREED_DATE  -- added new change reason code by ankit nug no. 144843
            END AS FINAL_CUSTOMER_AGREED_DATE,
            S_VIEW.SHIP_CONFIRM_DATE,
            S_VIEW.PICKUP_APPOINTMENT,
            S_VIEW.DEST_PICKUP_APPOINTMENT,
            S_VIEW.DELIVERY_APPOINTMENT,
            S_VIEW.STATUS,
            S_VIEW.CANCEL_REASON_CODE,
            S_VIEW.USERNAME,
            S_VIEW.HIST_CREATION_DATE,
            S_VIEW.LOCAL_ARRIVAL_DATE_214,
            S_VIEW.SHIP_TO_CITY,
            S_VIEW.SHIP_TO_STATE,
            S_VIEW.ZIP_CODE,
            S_VIEW.PARTY_SITE_NUMBER,
            S_VIEW.SALES_CHANNEL_CODE,
            S_VIEW.DTS_FLAG,
            S_VIEW.DTS_DC_CLASSIFICATION,
            S_VIEW.CARRIER_NAME,
            S_VIEW.ORDER_CREATION_DATE,
            S_VIEW.DEST_ORDER_CREATION_DATE,
            S_VIEW.SHIPMENT_GID,
            S_VIEW.ORDER_HEADER_STATUS,
            S_VIEW.BOTTLE_SIZE,
            S_VIEW.PACKAGE_SIZE,
            S_VIEW.WATER_TYPE,
            S_VIEW.PRODUCT,
            S_VIEW.PRODUCT_FAMILY,
            S_VIEW.PRODUCT_CATEGORY,
            S_VIEW.SALES_CATEGORY,
            S_VIEW.REASON_COMMENTS,
            S_VIEW.TRANSPORT_MODE_GID,
            S_VIEW.SHIPMENT_DROP,
            S_VIEW.EARLY_DELIVERY_DATE,
            S_VIEW.LATE_DELIVERY_DATE,
            S_VIEW.DELIVERY_WINDOW_START,
            S_VIEW.DELIVERY_WINDOW_END,
            S_VIEW.HISTORY_TYPE,
            S_VIEW.CHANGE_COUNTER,
            S_VIEW.ROLLUP_ORGANIZATION_CODE,
            S_VIEW.SUPPLY_CHAIN_REGION,
            S_VIEW.DOMAIN_NAME,
            S_VIEW.SHIP_TO_COUNTRY,
            S_VIEW.BUFFER_TIME_MINUTES,
            S_VIEW.CONTRACTUAL_LEAD_TIME,
            S_VIEW.CS_TEAM,
            S_VIEW.CUSTOMER_REGION,
            S_VIEW.CUSTOMER_LOCATION_GID,
            S_VIEW.SHIPPING_REGION,
            S_VIEW.CS_REPS,
            S_VIEW.SCHEDULED_FLAG,
            S_VIEW.STATUS_TYPE_GID,
            S_VIEW.STATUS_VALUE_GID,
            S_VIEW.OR_RATE_OFFERING_GID,
            S_VIEW.ORDER_RELEASE_INDICATOR,
            S_VIEW.NIAGARA_SALES_REGION,
            S_VIEW.RATE_GEO_GID,
            S_VIEW.X_LANE_GID,
            S_VIEW.FINAL_MILES,
            S_VIEW.OR_PENDING_REASON,
            S_VIEW.SK_ITEM_ID,
            S_VIEW.SH_RATE_OFFERING_GID,
            S_VIEW.SH_RATE_GEO_GID,
            S_VIEW.CANCELLED_QUANTITY,
            S_VIEW.SH_X_LANE_GID,
            S_VIEW.SH_TRANSPORT_MODE_GID,
            S_VIEW.SH_DEPLOYMENT_FLAG,
            S_VIEW.SCHEDULE_INSERT_DATE,
            S_VIEW.SCHEDULE_INSERT_DATE_PST,
            S_VIEW.SCHEDULE_INSERT_DATE_LOCAL,
            S_VIEW.SCHEDULE_UPDATE_DATE,
            S_VIEW.SCHEDULE_UPDATE_DATE_PST,
            S_VIEW.SCHEDULE_UPDATE_DATE_LOCAL,
            S_VIEW.SCHEDULE_UPDATE_USER,
            S_VIEW.CHECK_IN_TIME,
            S_VIEW.CHECK_OUT_TIME,
			S_VIEW.CHECK_IN_TIME_LOCAL,
            S_VIEW.CHECK_OUT_TIME_LOCAL,
            S_VIEW.ADDRESS,
            S_VIEW.DOCK_DOOR,
            S_VIEW.DOCK_DOOR_ALLOCATION,
            S_VIEW.STARTDATETIME,
            S_VIEW.FIRST_LOAD_TIME,
            S_VIEW.LAST_LOAD_TIME,
            S_VIEW.LOAD_TYPE,
            S_VIEW.CLOSE_TRAILER, 
            S_VIEW.CLOSE_TRAILER_LOCAL,
            S_VIEW.LATEST_CHANGE_FILTER, 
            CASE
                WHEN QTY_SHIPPED = QTY_REQUESTED THEN 'Y'
                ELSE 'N'
            END QTY_CORRECT,
            CASE
                WHEN LATEST_CHANGE_FILTER = 'Y'
                   AND HISTORY_TYPE = 'CREATE'
                   AND FINAL_REQUEST_DATE <= DEST_ORDER_CREATION_DATE --ORDER_CREATION_DATE
                    THEN 0
                WHEN LATEST_CHANGE_FILTER = 'Y' AND HISTORY_TYPE = 'CREATE' AND FINAL_REQUEST_DATE > DEST_ORDER_CREATION_DATE
                    THEN CAST(DATEDIFF(TO_DATE(FINAL_REQUEST_DATE),TO_DATE(DEST_ORDER_CREATION_DATE)) AS INT)
                WHEN ORIGINAL_REQUEST_DATE IS NULL THEN 0
                WHEN ORIGINAL_REQUEST_DATE <= DEST_ORDER_CREATION_DATE THEN 0
                WHEN DEST_ORDER_CREATION_DATE IS NULL THEN 0
                ELSE CAST(DATEDIFF(TO_DATE(ORIGINAL_REQUEST_DATE), TO_DATE(DEST_ORDER_CREATION_DATE)) AS INT)
            END LEAD_TIME_DAYS,
            CASE
                WHEN LATEST_CHANGE_FILTER = 'Y' THEN 1 / COUNT(ITEM) OVER(PARTITION BY ORDER_NUMBER, LATEST_CHANGE_FILTER, S_VIEW.STATUS)
                ELSE NULL
            END ORDER_COUNTER,
            CASE
                WHEN LATEST_CHANGE_FILTER = 'Y' THEN 1
                ELSE NULL
            END ITEM_COUNTER,
            ORIGINAL_REQUEST_DATE ORIGINAL_REQUEST_DATE,  
            S_VIEW.ORIGINAL_REQUEST_DATE     ORIGINAL_REQUEST_DATE_PST,
            S_VIEW.REQUEST_DATE_FROM         REQUEST_DATE_FROM, 
            S_VIEW.REQUEST_DATE_TO           REQUEST_DATE_TO,     
            CASE                                     
                WHEN LATEST_CHANGE_FILTER = 'Y'
                   AND HISTORY_TYPE = 'CREATE' THEN NULL
                WHEN LATEST_CHANGE_FILTER = 'Y' THEN
                        CASE
                            WHEN TO_DATE(S_VIEW.REQUEST_DATE_FROM) <> TO_DATE(S_VIEW.FINAL_REQUEST_DATE)
                               AND TO_DATE(S_VIEW.CUSTOMER_AGREED_DATE_FROM) = TO_DATE(S_VIEW.FINAL_CUSTOMER_AGREED_DATE) THEN
                                'Y'
                            ELSE NULL
                        END
                ELSE
                    CASE
                        WHEN TO_DATE(S_VIEW.REQUEST_DATE_FROM) <> TO_DATE(S_VIEW.REQUEST_DATE_TO)
                               AND TO_DATE(S_VIEW.CUSTOMER_AGREED_DATE_FROM) = TO_DATE(S_VIEW.CUSTOMER_AGRRED_DATE) THEN 'Y'
                        ELSE NULL
                    END
            END NIAGARA_CHANGE,
            S_VIEW.CUSTOMER_AGREED_DATE_TO   CUSTOMER_AGREED_DATE_TO,     
            S_VIEW.CUSTOMER_AGREED_DATE_FROM CUSTOMER_AGREED_DATE_FROM, 
            CASE
                WHEN LATEST_CHANGE_FILTER = 'Y' THEN 1 / COUNT(ITEM) OVER(PARTITION BY S_VIEW.SHIPMENT_GID, LATEST_CHANGE_FILTER, S_VIEW.STATUS)
                ELSE NULL
            END SHIPMENT_COUNTER,
            CASE
                WHEN LOAD_TYPE = 'CustomerPickup'
                    OR CARRIER_NAME = 'WAL-MART TRANSPORTATION, INC.'
                    OR ( S_VIEW.CUSTOMER = 'TALKING RAIN BEVERAGE COMPANY, INC.'
                   AND CARRIER_NAME = 'HARALAMBOS BEVERAGE CO' ) THEN FINAL_REQUEST_DATE
                ELSE
                    CASE
                            WHEN DELIVERY_WINDOW_END IS NOT NULL
                               AND ( DELIVERY_WINDOW_END > DELIVERY_APPOINTMENT
                                OR DELIVERY_APPOINTMENT IS NULL ) THEN DELIVERY_WINDOW_END
                            WHEN DELIVERY_APPOINTMENT IS NOT NULL
                               AND BUFFER_TIME_MINUTES IS NOT NULL THEN TIMESTAMPADD( MINUTE,BUFFER_TIME_MINUTES,DELIVERY_APPOINTMENT)
                            WHEN DELIVERY_APPOINTMENT IS NOT NULL THEN TIMESTAMPADD( HOUR,1,DELIVERY_APPOINTMENT )
                            ELSE NULL
                    END
            END  BUFFERED_APPOINTMENT_TIME, 
            S_VIEW.FG_BRAND,                  
            S_VIEW.DEF_ORG,                   
            S_VIEW.DEF_ROLLUP_ORG_CODE,       
            S_VIEW.ORDER_RELEASE_INSERT_DATE, 
            S_VIEW.LOCAL_ACTUAL_DEPARTURE,    
            S_VIEW.SHIP_TO_ORG,               
            S_VIEW.SHIP_TO_ROLLUP_ORG,        
            S_VIEW.SHIP_TO_SC_REGION,         
            S_VIEW.ORDER_TYPE_CODE,
            --142300			
			FIRST_VALUE(CHANGE_REASON_CODE, TRUE)
			OVER (
				PARTITION BY S_VIEW.ORDER_NUMBER
				ORDER BY
					CASE WHEN CHANGE_REASON_CODE IS NULL THEN 1 ELSE 0 END,
					HIST_CREATION_DATE ASC
			) AS FIRST_RESCHEDULE_REASON_CODE,     
            S_VIEW.LAST_RESCHEDULE_REASON_CODE, 
            S_VIEW.CHANGE_REASON_CODE,
            S_VIEW.REQUEST_DT_YEAR,
            S_VIEW.REQUEST_DT_WEEK,
            S_VIEW.WATER_TYPE_SC,
			S_VIEW.WATERTYPE,
            S_VIEW.PRODUCT_CATEGORY_SC,
            S_VIEW.CANCELLED_24PKEQCASES,
            S_VIEW.CANCELLED_CASES,
            S_VIEW.CANCELLED_PALLETS,
            S_VIEW.X_SHIPMENT_PRIORITY_CODE,
            S_VIEW.OSP,
            S_VIEW.MOSP,
            S_VIEW.ORG_REQUEST_DATE,
            S_VIEW.OR_INSERT_DATE_PST,
            S_VIEW.APPOINTMENT_PICKUP_PST,
            S_VIEW.OR_USER_DEFINED1_ICON_GID,
            S_VIEW.X_ORDERED_ITEM,
            S_VIEW.APPOINTMENT_DELIVERY_PST,
            S_VIEW.DELIVERY_WINDOW_START_LOCAL,
            S_VIEW.DELIVERY_WINDOW_END_LOCAL,
            S_VIEW.SUPPLY_CHAIN_LOADTYPE,
            S_VIEW.X_CREATED_ON_DT_SRC_TZ,
            S_VIEW.SHIP_CONFIRM_DATE_LOCAL,
            S_VIEW.CUSTOMER_ITEM,
            S_VIEW.FG_BUSINESS_UNIT,
            S_VIEW.FG_FLAVOR,
            S_VIEW.PRODUCT_TYPE,
            S_VIEW.DEMAND_CLASS_CODE,
            S_VIEW.AOP_RESULT,
            S_VIEW.GENPLAN_RESULT,
            S_VIEW.SH_USER_DEFINED1_ICON_GID,
            S_VIEW.SK_X_DESTINATION_ORGANIZATION_ID,
            S_VIEW.SK_INVENTORY_ORG_ID,
            S_VIEW.SK_X_UOM_ID,
            S_VIEW.ORIGINAL_REQUESTED_QTY,
            S_VIEW.SOA_SHIPPED_QUANTITY,
			S_VIEW.ACTUAL_SOA_SHIPPED_QUANTITY,
            S_VIEW.SOA_SHIPMENT_DATE,
            S_VIEW.SOA_CHECK_IN_TIME,
            S_VIEW.SOA_CHECK_OUT_TIME,
            S_VIEW.EQUIPMENT_NUMBER,
            S_VIEW.SHIPMENT_PRELOAD,
            S_VIEW.SK_X_SHIP_TO_ORG_ID,
            S_VIEW.ADDRESS_DESCRIPTION,
            S_VIEW.OREF_SCHEDULE_UPDATE_DATE,
            S_VIEW.OREF_SCHEDULE_INSERT_USER,
            S_VIEW.OREF_SCHEDULE_UPDATE_DATE_LOCAL,
            S_VIEW.OREF_SCHEDULE_INSERT_DATE_LOCAL,
            S_VIEW.OREF_SCHEDULE_UPDATE_DATE_PST,
            S_VIEW.OREF_SCHEDULE_INSERT_DATE_PST,
            S_VIEW.WITH_DRAW_USER_FLAG,
            S_VIEW.FINAL_REQUEST_DATE_PST,
            S_VIEW.FINAL_CUSTOMER_DELIVERY_DATE,
            S_VIEW.STANDARD_LEAD_TIME,     
            S_VIEW.MTO_FLG              MTO_FLG,        
            S_VIEW.ORIGINAL_REQUESTED_QTY_CASES,
            S_VIEW.CS_SCHEDULER,
            S_VIEW.PUA_REASON_CODE, 
            S_VIEW.EARLY_PICKUP_DATE, 
			S_VIEW.ORDERED_ON_DT,
			S_VIEW.OR_TRANSPORTATION_MODE_GID
        FROM
         (
		SELECT
		    --MODIFIED BY ANKIT, BUG NO. 144843
		    CASE WHEN SOURCE_APP_ID = 18 THEN 
			CASE
				WHEN B.HISTORY_TYPE = 'CREATE' THEN B.REQUEST_DATE_TEMP
				ELSE B.REQUEST_DATE
			END 
			ELSE REQUEST_DATE_TEMP	END		      
			AS REQUEST_DATE_FROM,
		    --MODIFIED BY ANKIT, BUG NO. 144843
 
			CASE 
    WHEN B.LATEST_CHANGE_FILTER = 'Y' THEN B.FINAL_REQUEST_DATE
    WHEN SOURCE_APP_ID = 18 THEN B.REQUEST_DATE_TEMP
    ELSE B.REQUEST_DATE
END AS REQUEST_DATE_TO	,
		    
			CASE 
    WHEN B.HISTORY_TYPE = 'CREATE' THEN B.REQUEST_DATE_TEMP
    WHEN SOURCE_APP_ID = 18 THEN B.CUSTOMER_AGRRED_DATE
    ELSE B.CUSTOMER_AGREED_DATE_TEMP
END AS CUSTOMER_AGREED_DATE_FROM,
		    
			CASE 
    WHEN B.HISTORY_TYPE = 'CREATE' THEN B.REQUEST_DATE_TEMP
    WHEN B.LATEST_CHANGE_FILTER = 'Y' THEN B.FINAL_CUSTOMER_AGREED_DATE
    WHEN SOURCE_APP_ID = 18 THEN B.CUSTOMER_AGREED_DATE_TEMP
    ELSE B.CUSTOMER_AGRRED_DATE
END AS CUSTOMER_AGREED_DATE_TO,
		
			CASE
				WHEN B.HISTORY_TYPE = 'CREATE' AND B.LATEST_CHANGE_FILTER = 'Y' THEN B.REQUEST_DATE_TEMP
				ELSE B.ORIGINAL_REQUEST_DATE_TEMP
			END AS ORIGINAL_REQUEST_DATE,
			CASE
				WHEN B.HISTORY_TYPE = 'UPDATE'
					AND (
						CAST(
							from_utc_timestamp(
								CASE
									WHEN B.HISTORY_TYPE = 'CREATE' THEN B.REQUEST_DATE_TEMP
									WHEN B.LATEST_CHANGE_FILTER = 'Y' THEN B.FINAL_CUSTOMER_AGREED_DATE
									ELSE B.CUSTOMER_AGREED_DATE_TEMP
								END,
								'America/Los_Angeles'
							) AS DATE
						)
<>
						CAST(
							from_utc_timestamp(
								CASE
									WHEN B.HISTORY_TYPE = 'CREATE' THEN B.REQUEST_DATE_TEMP 
                   WHEN B.LATEST_CHANGE_FILTER = 'Y' THEN B.CUSTOMER_AGREED_DATE_TEMP
									ELSE B.CUSTOMER_AGRRED_DATE
								END,
								'America/Los_Angeles'
							) AS DATE
						)
						OR
						CAST(
							from_utc_timestamp(
								CASE
									WHEN B.LATEST_CHANGE_FILTER = 'Y' THEN B.FINAL_REQUEST_DATE
									ELSE B.REQUEST_DATE_TEMP
								END,
								'America/Los_Angeles'
							) AS DATE
						)
<>
						CAST(
							from_utc_timestamp(
								CASE
									WHEN B.HISTORY_TYPE = 'CREATE' THEN B.REQUEST_DATE_TEMP
                  WHEN B.LATEST_CHANGE_FILTER = 'Y' THEN B.REQUEST_DATE_TEMP
									ELSE B.REQUEST_DATE
								END,
								'America/Los_Angeles'
							) AS DATE
						)
					)
				THEN B.DATE_CHANGE_REASON_CODE
				ELSE NULL
			END AS CHANGE_REASON_CODE,
			CASE
				WHEN UPPER(TRIM(B.CUSTOMER)) LIKE '%ESSENTIA%' THEN
					COALESCE(B.PICKUP_APPOINTMENT, B.SHIP_CONFIRM_DATE, B.SOA_SHIPMENT_DATE)
		
				WHEN B.LOAD_TYPE = 'CustomerPickup'
					OR UPPER(TRIM(B.CARRIER_NAME)) = 'WAL-MART TRANSPORTATION, INC.'
					OR (
							UPPER(TRIM(B.CARRIER_NAME)) = 'HARALAMBOS BEVERAGE CO'
							AND UPPER(TRIM(B.CUSTOMER)) = 'TALKING RAIN BEVERAGE COMPANY, INC.'
						)
				THEN LEAST(B.PICKUP_APPOINTMENT, B.SHIP_CONFIRM_DATE, B.SOA_SHIPMENT_DATE) --Modified column derivation by Swaroopa 02/19/2025
		
				ELSE LEAST(B.LOCAL_ARRIVAL_DATE_214, B.SHIP_CONFIRM_DATE, B.SOA_SHIPMENT_DATE) --Modified column to get least date by Swaroopa 02/19/2025
			END AS FINAL_CUSTOMER_DELIVERY_DATE,
			CASE
				WHEN B.MTO_FLG = 1 THEN 14
				ELSE B.CONTRACTUAL_LEAD_TIME
			END AS STANDARD_LEAD_TIME,
		
			B.*
          FROM
             ( 
			SELECT
			    CASE WHEN SOURCE_APP_ID = 18 THEN 
				FIRST_VALUE(A.REQUEST_DATE)
				OVER (
					PARTITION BY A.ORDER_NUMBER, A.LINE_NUMBER
					ORDER BY HIST_CREATION_DATE DESC  --MODIFIED BY ANKIT, BUG NO. 144843
					ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
				) 
				ELSE 
				CASE
    WHEN LAG(A.HISTORY_TYPE) OVER (
            PARTITION BY A.ORDER_NUMBER, A.LINE_NUMBER
            ORDER BY HIST_CREATION_DATE
         ) = 'CREATE'
    THEN A.REQUEST_DATE
    ELSE
        FIRST_VALUE(A.REQUEST_DATE)
        OVER (
            PARTITION BY A.ORDER_NUMBER, A.LINE_NUMBER
            ORDER BY HIST_CREATION_DATE ASC
            ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
        )
END END AS REQUEST_DATE_TEMP, --MODIFIED BY ANKIT, BUG NO. 144843
	            
				CASE WHEN SOURCE_APP_ID = 18 THEN 
				FIRST_VALUE(A.CUSTOMER_AGRRED_DATE)
				OVER (
					PARTITION BY A.ORDER_NUMBER, A.LINE_NUMBER
					ORDER BY HIST_CREATION_DATE DESC   --MODIFIED BY ANKIT, BUG NO. 144843
					ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
				)
				ELSE
				CASE 
    WHEN LAG(A.HISTORY_TYPE) OVER (
            PARTITION BY A.ORDER_NUMBER, A.LINE_NUMBER
            ORDER BY HIST_CREATION_DATE
         ) = 'CREATE'
    THEN A.CUSTOMER_AGRRED_DATE
    ELSE
        FIRST_VALUE(A.CUSTOMER_AGRRED_DATE)
        OVER (
            PARTITION BY A.ORDER_NUMBER, A.LINE_NUMBER
            ORDER BY HIST_CREATION_DATE
            ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
        )
END END AS CUSTOMER_AGREED_DATE_TEMP, --MODIFIED BY ANKIT, BUG NO. 144843
	
				NTH_VALUE(A.REQUEST_DATE, 2)
				OVER (
					PARTITION BY A.ORDER_NUMBER, A.LINE_NUMBER
					ORDER BY HIST_CREATION_DATE ASC
					ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
				) AS ORIGINAL_REQUEST_DATE_TEMP,
	
				CASE
					WHEN ROW_NUMBER() OVER (
							PARTITION BY A.ORDER_NUMBER, A.LINE_NUMBER
							ORDER BY HIST_CREATION_DATE ASC
						)
						= COUNT(*) OVER (PARTITION BY A.ORDER_NUMBER, A.LINE_NUMBER)
					THEN 'Y'
					ELSE 'N'
				END AS LATEST_CHANGE_FILTER,
	
				FIRST_VALUE(A.ORDERED_QTY)
				OVER (
					PARTITION BY A.ORDER_NUMBER, A.LINE_NUMBER
					ORDER BY HIST_CREATION_DATE ASC
					ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
				) AS ORIGINAL_REQUESTED_QTY,--142300
	
				FIRST_VALUE(A.ORDERED_QTY_CASES)
				OVER (
					PARTITION BY A.ORDER_NUMBER, A.LINE_NUMBER
					ORDER BY HIST_CREATION_DATE 
					ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
				) AS ORIGINAL_REQUESTED_QTY_CASES, --142300
	
				CASE
					WHEN A.STATUS in ('CANCELLED','CANCELED') THEN
						FIRST_VALUE(A.FIRST_CANCELLED_REASON_CODE)
						OVER (PARTITION BY A.ORDER_NUMBER, A.LINE_NUMBER ORDER BY HIST_CREATION_DATE ASC)
					ELSE
						FIRST_VALUE(A.FIRST_REASON_CODE)
						OVER (PARTITION BY A.ORDER_NUMBER, A.LINE_NUMBER ORDER BY HIST_CREATION_DATE ASC)
				END AS PUA_REASON_CODE,
	
				A.*
        FROM
		(
		SELECT
			'CREATE' AS HISTORY_TYPE, OA.SOURCE_APP_ID,  --MODIFIED BY ANKIT, BUG NO. 144843
			OA.CREATED_ON_DT AS HIST_CREATION_DATE,
			OA.TRIP_ID AS TRIP_ID,
			OA.STOP_NUMBER AS STOP_NUMBER,
			C.CUSTOMER_NAME AS CUSTOMER,
			OA.PURCH_ORDER_NUM AS CUST_PO,
			OA.SALES_ORDER_NUM AS ORDER_NUMBER,
			CONCAT_WS('.',OA.SALES_ORDER_ITEM,CAST(OA.X_SHIPMENT_NUMBER AS string)) AS LINE_NUMBER,
			OA.DELIVERY_ID AS DELIVERY_NUMBER,
            FND.MEANING FREIGHT_METHOD,
			IO.ORGANIZATION_CODE AS ORGANIZATION_CODE,
			I.PRODUCT_NUM AS ITEM,
			I.X_DEPLOYMENT_FLAG AS DEPLOYMENT_FLAG,
			OA.SALES_UOM_CODE AS UOM,
			OA.SALES_QTY AS QTY_REQUESTED,
			(UOMD.CONVERSION_RATE_TO_24PK * OA.SALES_QTY) AS CASES_24PKEQ_REQUESTED,
			(UOMD.CONVERSION_RATE_TO_CASE * OA.SALES_QTY) AS ACTUAL_CASES_REQUESTED,
			(UOMD.CONVERSION_RATE_TO_PALLET * OA.SALES_QTY) AS ACTUAL_PALLETS_REQUESTED,
			OA.TOTAL_SHIPPED_QTY AS QTY_SHIPPED,
			(UOMD.CONVERSION_RATE_TO_24PK * OA.TOTAL_SHIPPED_QTY) AS CASES_24PKEQ_SHIPPED,
			(UOMD.CONVERSION_RATE_TO_CASE * OA.TOTAL_SHIPPED_QTY) AS ACTUAL_CASES_SHIPPED,
			(UOMD.CONVERSION_RATE_TO_PALLET * OA.TOTAL_SHIPPED_QTY) AS ACTUAL_PALLETS_SHIPPED,
			(OA.SALES_QTY - OA.TOTAL_SHIPPED_QTY) AS QTY_SHORT_SHIPPED,
			(UOMD.CONVERSION_RATE_TO_24PK * (OA.SALES_QTY - NVL(OA.TOTAL_SHIPPED_QTY,0))) AS CASES_24PKEQ_SHORT_SHIPPED,
			(UOMD.CONVERSION_RATE_TO_CASE * (OA.SALES_QTY - OA.TOTAL_SHIPPED_QTY)) AS ACTUAL_CASES_SHORT_SHIPPED,
			(UOMD.CONVERSION_RATE_TO_PALLET * (OA.SALES_QTY - OA.TOTAL_SHIPPED_QTY)) AS ACTUAL_PALLETS_SHORT_SHIPPED,
			OA.X_OLA_ATTRIBUTE7 AS SHORT_SHIP_REASON_CODE,
			CAST(NULL AS STRING) AS DATE_CHANGE_REASON_CODE,
			OA.X_REQUEST_DATE AS REQUEST_DATE,
			OA.X_REQUEST_DATE AS FINAL_REQUEST_DATE,
			OA.X_OLA_ATTRIBUTE11 AS ATTRIBUTE11,
			OA.X_OLA_ATTRIBUTE12 AS ORIGINAL_SCHEDULE_SHIP_DATE,
			--Start: Changes by Ankit N on 2/19 for reversing timezone conversion for Promise Date
   			--OA.X_PROMISE_DATE_CUST_TZ   AS CUSTOMER_AGRRED_DATE,  
			--OA.X_PROMISE_DATE_CUST_TZ  AS FINAL_CUSTOMER_AGREED_DATE, 
			--MODIFIED BY ANKIT, BUG NO. 144843
   			to_utc_timestamp(OA.X_PROMISE_DATE_CUST_TZ, 'America/Los_Angeles')  AS CUSTOMER_AGRRED_DATE, 
			to_utc_timestamp(OA.X_PROMISE_DATE_CUST_TZ ,'America/Los_Angeles') AS FINAL_CUSTOMER_AGREED_DATE,
			--MODIFIED BY ANKIT, BUG NO. 144843
			--End: Changes by Ankit N on 2/19
			OA.SHIP_CONFIRM_DATE_PST AS SHIP_CONFIRM_DATE,
			OA.APPOINTMENT_PICKUP_LOCAL AS PICKUP_APPOINTMENT,
			OA.APPOINTMENT_PICKUP_PST,
			CASE 
				WHEN C.COUNTRY_CODE = 'US' THEN 
					CASE 
						WHEN (IO.X_TIME_ZONE_GID = C.TIME_ZONE_GID 
							OR IO.X_TIME_ZONE_GID IN ('-', 'NULL', '') 
							OR C.TIME_ZONE_GID IN ('-', 'NULL', ''))
						THEN OA.APPOINTMENT_PICKUP_LOCAL
						ELSE to_timestamp(
								from_utc_timestamp(
									to_utc_timestamp(
										CAST(OA.APPOINTMENT_PICKUP_LOCAL AS TIMESTAMP),
										COALESCE(NULLIF(IO.X_TIME_ZONE_GID, '-'), 'UTC')
									),
									COALESCE(NULLIF(C.TIME_ZONE_GID, '-'), 'UTC')
								)
							)
					END
				ELSE OA.APPOINTMENT_PICKUP_LOCAL
			END AS DEST_PICKUP_APPOINTMENT,
			OA.APPOINTMENT_DELIVERY_LOCAL AS DELIVERY_APPOINTMENT,
			OS.STATUS_CODE AS STATUS,
			CAST(NULL AS STRING) AS CANCEL_REASON_CODE,
			CAST(NULL AS STRING) AS USERNAME,
			LEAST(
				CAST(COALESCE(WC_214_TMP.DATE_214, OA.X1_STATUS_DATE_WEB_LOCAL, OA.X1_STATUS_DATE_214_LOCAL) AS TIMESTAMP),
				CAST(COALESCE(WC_214_TMP.DATE_214, OA.X1_STATUS_DATE_214_LOCAL, OA.X1_STATUS_DATE_WEB_LOCAL) AS TIMESTAMP)
			) AS LOCAL_ARRIVAL_DATE_214,
			C.CITY_CODE AS SHIP_TO_CITY,
			C.STATEPROVINCE_CODE AS SHIP_TO_STATE,
			C.POSTAL_CODE AS ZIP_CODE,
			C.PARTY_SITE_NUMBER AS PARTY_SITE_NUMBER,
			C.DISTRIBUTION_CHANNEL_CODE AS SALES_CHANNEL_CODE,
			C.DSD AS DTS_FLAG,
			DECODE(C.DSD, 'Y', DECODE(C.DISTRIBUTION_CHANNEL_CODE, 'CLUB', 'DTS Club', 'DTS Non-Club'), 'DC') AS DTS_DC_CLASSIFICATION,
			OA.CARRIER_NAME AS CARRIER_NAME,
			OA.CREATED_ON_DT AS ORDER_CREATION_DATE,
			OA.X_CREATED_ON_DT_CUST_TZ AS DEST_ORDER_CREATION_DATE,
			OA.SHIPMENT_GID,
			HOS.STATUS_CODE AS ORDER_HEADER_STATUS,
			I.X_BOTTLE_SIZE AS BOTTLE_SIZE,
			I.X_PACK_SIZE AS PACKAGE_SIZE,
			I.X_WATER_TYPE AS WATER_TYPE,
			I.X_WATER_TYPE2 AS WATERTYPE,
			I.X_WATER_TYPE_SC AS WATER_TYPE_SC,
			I.X_PRODUCT AS PRODUCT,
			I.X_PRODUCT_FAMILY AS PRODUCT_FAMILY,
			I.X_PRODUCT_CATEGORY AS PRODUCT_CATEGORY,
			I.X_PRODUCT_CATEGORY_SC AS PRODUCT_CATEGORY_SC,
			I.X_FG_SALES_CATGORY AS SALES_CATEGORY,
			CAST(NULL AS STRING) AS REASON_COMMENTS,
			OA.LOADTYPE AS LOAD_TYPE,
			OA.OR_TRANSPORT_MODE_GID AS TRANSPORT_MODE_GID,
			CASE 
				WHEN OA.SH_TRANSPORT_MODE_GID = 'NBL.DROP' THEN 'DROP' 
				ELSE 'NOT-DROP' 
			END AS SHIPMENT_DROP,
			OA.EARLY_DELIVERY_DATE_LOCAL AS EARLY_DELIVERY_DATE,
			OA.LATE_DELIVERY_DATE_LOCAL AS LATE_DELIVERY_DATE,
			CASE WHEN OA.EARLY_DELIVERY_DATE_LOCAL <> OA.LATE_DELIVERY_DATE_LOCAL THEN OA.EARLY_DELIVERY_DATE_LOCAL END AS DELIVERY_WINDOW_START,
			CASE WHEN OA.EARLY_DELIVERY_DATE_LOCAL <> OA.LATE_DELIVERY_DATE_LOCAL THEN OA.LATE_DELIVERY_DATE_LOCAL END AS DELIVERY_WINDOW_END,
			0 AS CHANGE_COUNTER,
			IO.X_ROLLUP_ORGANIZATION_CODE AS ROLLUP_ORGANIZATION_CODE,
			IO.X_SUPPLY_CHAIN_REGION AS SUPPLY_CHAIN_REGION,
			DECODE(OU.ORG_UNIT_NAME, 'Embotelladora Niagara de Mexico BU', 'NBL/MX', 'NBL') AS DOMAIN_NAME,
			C.COUNTRY_CODE AS SHIP_TO_COUNTRY,
			CAST(NVL(DECODE(C.CUST_ATTRIBUTE6, '-', 60, 'Unspecified', 60, C.CUST_ATTRIBUTE6), 60) AS INT) AS BUFFER_TIME_MINUTES,
            CAST(NVL(DECODE(C.SITE_USE_ATTRIBUTE18, '-', 7, 'Unspecified', 7, C.SITE_USE_ATTRIBUTE18),60) AS INT) AS CONTRACTUAL_LEAD_TIME,
			C.NEW_ORGANIC_CUSTOMER AS CS_TEAM,
			C.CUSTOMER_REGION AS CUSTOMER_REGION,
			C.LOCATION_GID AS CUSTOMER_LOCATION_GID,
			IO.X_SHIPPING_REGION AS SHIPPING_REGION,
			C.SITE_USE_ATTRIBUTE3 AS CS_REPS,
			CAST(NULL AS STRING) AS SCHEDULED_FLAG,
			CAST(NULL AS STRING) AS STATUS_TYPE_GID,
			OA.STATUS_VALUE_GID,
			OA.OR_RATE_OFFERING_GID,
			OA.OR_INDICATOR AS ORDER_RELEASE_INDICATOR,
			S.NIAGARA_SALES_REGION,
			NVL(OA.OR_RATE_GEO_GID, '-') AS RATE_GEO_GID,
			NVL(OA.X_LANE_GID, '-') AS X_LANE_GID,
			NVL(OA.OR_TRANSPORT_MODE_GID, '-') AS OR_TRANSPORT_MODE_GID,
			NVL(DPCM.FINAL_MILES, 0) AS FINAL_MILES,
			OA.OR_PENDING_REASON,
			OA.SK_X_INVENTORY_PRODUCT_ID,
			OA.SH_RATE_OFFERING_GID,
			OA.SH_RATE_GEO_GID,
			OA.CANCELLED_QTY AS CANCELLED_QUANTITY,
			(UOMD.CONVERSION_RATE_TO_24PK * OA.CANCELLED_QTY) AS CANCELLED_24PKEQCASES,
			(UOMD.CONVERSION_RATE_TO_CASE * OA.CANCELLED_QTY) AS CANCELLED_CASES,
			(UOMD.CONVERSION_RATE_TO_PALLET * OA.CANCELLED_QTY) AS CANCELLED_PALLETS,
			OA.X_LANE_GID AS SH_X_LANE_GID,
			OA.SH_TRANSPORT_MODE_GID AS SH_TRANSPORT_MODE_GID,
			OA.DEPLOYMENT_FLAG AS SH_DEPLOYMENT_FLAG,
			OA.SCHEDULE_INSERT_DATE,
			OA.SCHEDULE_INSERT_DATE_PST,
			OA.SCHEDULE_INSERT_DATE_LOCAL,
			CAST(OA.SCHEDULE_UPDATE_DATE AS DATE) AS SCHEDULE_UPDATE_DATE,
			CAST(OA.SCHEDULE_UPDATE_DATE_PST AS DATE)  AS SCHEDULE_UPDATE_DATE_PST,
			CAST(OA.SCHEDULE_UPDATE_DATE_LOCAL AS DATE)  AS SCHEDULE_UPDATE_DATE_LOCAL,
			OA.SCHEDULE_UPDATE_USER,
			OA.CHECK_IN_TIME,
			OA.CHECK_OUT_TIME,
			OA.CHECK_IN_TIME_LOCAL,
			OA.CHECK_OUT_TIME_LOCAL,
			C.ADDRESS,
			OA.DOCK_DOOR,
			OA.DOCK_DOOR_ALLOCATION,
			OA.STARTDATETIME,
			OA.FIRST_LOAD_TIME,
			OA.LAST_LOAD_TIME,
			OA.CLOSE_TRAILER,
			OA.CLOSE_TRAILER_LOCAL,
			I.X_FG_BRAND AS FG_BRAND,
			CAST(NULL AS STRING) AS DEF_ORG,
			OA.DEF_ROLLUP_ORG_CODE AS DEF_ROLLUP_ORG_CODE,
			OA.OR_INSERT_DATE_LOCAL AS ORDER_RELEASE_INSERT_DATE,
			OA.OR_INSERT_DATE_PST AS OR_INSERT_DATE_PST,
			OA.DEST_ACTUAL_DEPARTURE_LOCAL AS LOCAL_ACTUAL_DEPARTURE,
			SHIP_TO.ORGANIZATION_CODE AS SHIP_TO_ORG,
			SHIP_TO.X_ROLLUP_ORGANIZATION_CODE AS SHIP_TO_ROLLUP_ORG,
			SHIP_TO.X_SUPPLY_CHAIN_REGION AS SHIP_TO_SC_REGION,
			OT.XACT_SUBTYPE_CODE AS ORDER_TYPE_CODE,
			CAST(NULL AS STRING) AS LAST_RESCHEDULE_REASON_CODE,
			REQ_WEEK.MCAL_YEAR AS REQUEST_DT_YEAR,
			REQ_WEEK.MCAL_WEEK AS REQUEST_DT_WEEK,
			I.SK_INV_MASTER_ITEMS_ID AS SK_ITEM_ID,
			OA.X_SHIPMENT_PRIORITY_CODE,
			OA.SK_OSP_ID AS OSP,
			OA.SK_MOSP_ID AS MOSP,
			OA.ORG_REQUEST_DATE,
			OA.OR_USER_DEFINED3_ICON_GID AS OR_USER_DEFINED1_ICON_GID,
			C.TIME_ZONE_GID,
			OA.X_ORDERED_ITEM AS X_ORDERED_ITEM,
			OA.APPOINTMENT_DELIVERY_PST,
			CASE 
				WHEN OA.EARLY_DELIVERY_DATE_PST <> OA.LATE_DELIVERY_DATE_PST THEN OA.EARLY_DELIVERY_DATE_PST 
			END AS DELIVERY_WINDOW_START_LOCAL,
			CASE 
				WHEN OA.EARLY_DELIVERY_DATE_PST <> OA.LATE_DELIVERY_DATE_PST THEN OA.LATE_DELIVERY_DATE_PST 
			END AS DELIVERY_WINDOW_END_LOCAL,
			OA.SUPPLY_CHAIN_LOADTYPE,
			OA.X_CREATED_ON_DT_SRC_TZ,
			OA.SHIP_CONFIRM_DATE_LOCAL,
			I.X_CUSTOMER_ITEM_NUMBER AS CUSTOMER_ITEM,
			I.X_FG_BUSINESS_UNIT AS FG_BUSINESS_UNIT,
			I.X_FG_FLAVOR AS FG_FLAVOR,
			I.X_WATER_TYPE AS PRODUCT_TYPE,
			OA.X_OLA_DEMAND_CLASS_CODE AS DEMAND_CLASS_CODE,
			OA.X_OLA_ATTRIBUTE13 AS AOP_RESULT,
			OA.X_OLA_ATTRIBUTE14 AS GENPLAN_RESULT,
			OA.SH_USER_DEFINED1_ICON_GID,
			OA.SK_X_DESTINATION_ORGANIZATION_ID AS SK_X_DESTINATION_ORGANIZATION_ID,
			OA.SK_INVENTORY_ORG_ID AS SK_INVENTORY_ORG_ID,
			OA.SK_X_UOM_ID AS SK_X_UOM_ID,
			OA.ORDERED_QTY,
			CASE 
				WHEN OA.SOA_SHIPPED_QUANTITY > OA.SALES_QTY THEN OA.SALES_QTY 
				ELSE OA.SOA_SHIPPED_QUANTITY 
			END AS SOA_SHIPPED_QUANTITY,
			UOMD.CONVERSION_RATE_TO_CASE * (CASE 
												WHEN OA.SOA_SHIPPED_QUANTITY > OA.SALES_QTY THEN OA.SALES_QTY 
												ELSE OA.SOA_SHIPPED_QUANTITY 
											END) 
			AS ACTUAL_SOA_SHIPPED_QUANTITY,
			OA.SOA_SHIPMENT_DATE,
			OA.SOA_CHECK_IN_TIME,
			OA.SOA_CHECK_OUT_TIME,
			OA.EQUIPMENT_NUMBER,
			OA.SHIPMENT_PRELOAD,
			OA.SK_X_SHIP_TO_ORG_ID AS SK_X_SHIP_TO_ORG_ID,
			C.ADDRESS_DESCRIPTION,
			OA.OREF_SCHEDULE_UPDATE_DATE,
			OA.OREF_SCHEDULE_INSERT_USER,
			OA.OREF_SCHEDULE_UPDATE_DATE_LOCAL,
			OA.OREF_SCHEDULE_INSERT_DATE_LOCAL,
			OA.OREF_SCHEDULE_UPDATE_DATE_PST,
			OA.OREF_SCHEDULE_INSERT_DATE_PST,
			OA.WITH_DRAW_USER_FLAG,
			OA.X_FINAL_REQUEST_DATE_PST AS FINAL_REQUEST_DATE_PST,
			CASE 
				WHEN OA.X_OLA_ATTRIBUTE16 = 'MTO' THEN 1 
				ELSE 0 
			END AS MTO_FLG,
			(UOMD.CONVERSION_RATE_TO_CASE * OA.ORDERED_QTY) AS ORDERED_QTY_CASES,
			C.SITE_USE_ATTRIBUTE4 AS CS_SCHEDULER,
			CAST(NULL AS STRING) AS FIRST_REASON_CODE,
			CAST(NULL AS STRING) AS FIRST_CANCELLED_REASON_CODE,
			OA.EARLY_PICKUP_DATE_LOCAL AS EARLY_PICKUP_DATE,
			OA.ORDERED_ON_DT,
			OA.OR_TRANSPORT_MODE_GID AS OR_TRANSPORTATION_MODE_GID
		FROM NBL_FUSION_DW.FACT_SALES_ORDER_LINE OA
		LEFT JOIN NBL_FUSION_DW.DIM_UNIT_OF_MEASURE_D UOMD
			ON OA.SK_X_UOM_ID = UOMD.SK_UNIT_OF_MEASURE_D_ID
		LEFT JOIN NBL_FUSION_DW.DIM_OM_LINE_STATUS OS
			ON OA.SK_X_ORDER_LINE_STATUS_ID = OS.SK_LINE_STATUS_ID
		LEFT JOIN NBL_FUSION_DW.DIM_OM_STATUS HOS
			ON OA.SK_HEADER_STATUS_ID = HOS.SK_STATUS_ID 
		LEFT JOIN NBL_FUSION_DW.DIM_INV_MASTER_ITEMS I
			ON OA.SK_X_INVENTORY_PRODUCT_ID = I.SK_INV_MASTER_ITEMS_ID
		LEFT JOIN NBL_FUSION_DW.DIM_HR_ORG IO
			ON OA.SK_INVENTORY_ORG_ID = IO.SK_ORG_ID
		LEFT JOIN NBL_FUSION_DW.DIM_HR_ORG OU
			ON IO.X_OPERATING_UNIT = OU.ORGANIZATION_ID
		LEFT JOIN NBL_FUSION_DW.DIM_CUST_SITE_USE C
			ON OA.SK_X_SHIP_TO_ORG_ID = C.SK_CUST_SITE_USE_ID
		LEFT JOIN NBL_FUSION_DW.DIM_SALES_STATE S
			ON OA.SK_X_SALES_STATE_ID = S.SK_SALES_STATE_ID
		LEFT JOIN NBL_FUSION_DW.DIM_HR_ORG SHIP_TO
			ON OA.SK_X_DESTINATION_ORGANIZATION_ID = SHIP_TO.SK_ORG_ID
		LEFT JOIN NBL_FUSION_DW.DIM_OM_TRANSACTION_TYPE OT
			ON OA.SK_XACT_TYPE_ID = OT.SK_OM_TRANSACTION_TYPE_ID
			/*LEFT JOIN NBLBI_FND_COMMON.DIM_FND_LOOKUP_VALUES FND
			ON OA.FREIGHT_TERMS_ID = FND.LOOKUP_CODE
			AND FND.LOOKUP_TYPE = 'WSH_FREIGHT_CHARGE_TERMS'*/
		--In fusion freight terms are in msc_Sr_lookup_values ,hence using the table directly
		LEFT JOIN nblbi_fusion_fnd_common.MSC_SR_LOOKUP_VALUES_TL FND
		ON FND.LOOKUPCODE = OA.FREIGHT_TERMS_ID
		AND FND.LOOKUPTYPE = 'WSH_FREIGHT_CHARGE_TERMS'
		AND FND.LANGUAGE = 'US'
		LEFT JOIN NBL_FUSION_DW.DIM_MCAL_PLANNING_WEEK REQ_WEEK
			ON OA.SK_X_REQUEST_DT_PLNG_WK_ID = REQ_WEEK.ROW_WID
		LEFT JOIN NBL_FUSION_DW.DIM_PLANT_TO_CUSTOMER_MILES DPCM
			ON IO.ORGANIZATION_CODE = DPCM.PLANT
		AND C.POSTAL_CODE = DPCM.DEST_ZIPCODE
		LEFT JOIN NBLBI_FILES.WC_214_FIX_BY_DELIVERY_ID_TMP WC_214_TMP
			ON OA.DELIVERY_ID = WC_214_TMP.DELIVERY_ID
		WHERE OA.BOOKING_FLG = 'Y' --AND OA.SALES_ORDER_NUM  IN ('19963107','41140946') 
			AND OA.SALES_ORDER_NUM IN (SELECT SALES_ORDER_NUM AS ORDER_NUMBER 
										FROM (SELECT SALES_ORDER_NUM,'FACT_SALES_ORDER_LINE' X_CUSTOM ,CURRENT_DATE() AS W_INSERT_DT
												FROM NBL_FUSION_DW.FACT_SALES_ORDER_LINE
										
												UNION ALL
												SELECT ORDER_NUMBER,'FACT_ORDER_HISTORY_ACTIVITY' X_CUSTOM ,CURRENT_DATE() AS W_INSERT_DT
												FROM NBL_FUSION_DW.FACT_ORDER_HISTORY_ACTIVITY
											
											)
										)
			AND OA.X_LINE_CATEGORY_CODE <> 'RETURN'
			
			AND HOS.STATUS_CODE <> 'ENTERED'
			AND OT.XACT_SUBTYPE_CODE IN ('MX_DTS','MX_WAREHOUSE XFER','BOP SHIPMENT','DTS','STANDARD','WAREHOUSE XFER','MX_STANDARD')
		
		UNION ALL
		
		SELECT
			'UPDATE' HISTORY_TYPE, OA.source_app_id1 SOURCE_APP_ID,  --MODIFIED BY ANKIT, BUG NO. 144843
			OA.HIST_CREATION_DATE HIST_CREATION_DATE,
			OA.TRIP_ID TRIP_ID,
			OA.STOP_NUMBER STOP_NUMBER,
			C.CUSTOMER_NAME CUSTOMER,
			OA.PURCH_ORDER_NUM CUST_PO,
			OA.SALES_ORDER_NUM ORDER_NUMBER,
			CONCAT_WS('.',OA.SALES_ORDER_ITEM, CAST(OA.X_SHIPMENT_NUMBER AS string)) AS LINE_NUMBER,
			OA.DELIVERY_ID DELIVERY_NUMBER,
            FND.MEANING FREIGHT_METHOD,
			IO.ORGANIZATION_CODE ORGANIZATION_CODE,
			I.PRODUCT_NUM ITEM,
			I.X_DEPLOYMENT_FLAG DEPLOYMENT_FLAG,
			OA.SALES_UOM_CODE UOM,
			OA.SALES_QTY QTY_REQUESTED,
			(UOMD.CONVERSION_RATE_TO_24PK * OA.SALES_QTY) CASES_24PKEQ_REQUESTED,
			(UOMD.CONVERSION_RATE_TO_CASE * OA.SALES_QTY) ACTUAL_CASES_REQUESTED,
			(UOMD.CONVERSION_RATE_TO_PALLET * OA.SALES_QTY) ACTUAL_PALLETS_REQUESTED,
			OA.TOTAL_SHIPPED_QTY QTY_SHIPPED,
			(UOMD.CONVERSION_RATE_TO_24PK * OA.TOTAL_SHIPPED_QTY) CASES_24PKEQ_SHIPPED,
			(UOMD.CONVERSION_RATE_TO_CASE * OA.TOTAL_SHIPPED_QTY) ACTUAL_CASES_SHIPPED,
			(UOMD.CONVERSION_RATE_TO_PALLET * OA.TOTAL_SHIPPED_QTY) ACTUAL_PALLETS_SHIPPED,
			(OA.SALES_QTY - OA.TOTAL_SHIPPED_QTY) QTY_SHORT_SHIPPED,
			(UOMD.CONVERSION_RATE_TO_24PK * (OA.SALES_QTY - NVL(OA.TOTAL_SHIPPED_QTY,0))) CASES_24PKEQ_SHORT_SHIPPED,
			(UOMD.CONVERSION_RATE_TO_CASE * (OA.SALES_QTY - OA.TOTAL_SHIPPED_QTY)) ACTUAL_CASES_SHORT_SHIPPED,
			(UOMD.CONVERSION_RATE_TO_PALLET * (OA.SALES_QTY - OA.TOTAL_SHIPPED_QTY)) ACTUAL_PALLETS_SHORT_SHIPPED,
			OA.X_OLA_ATTRIBUTE7 SHORT_SHIP_REASON_CODE,
			OA.REASON_CODE DATE_CHANGE_REASON_CODE,
			OA.REQUEST_DATE REQUEST_DATE,
			OA.X_REQUEST_DATE FINAL_REQUEST_DATE,
			OA.X_OLA_ATTRIBUTE11 ATTRIBUTE11,
			OA.X_OLA_ATTRIBUTE12 ORIGINAL_SCHEDULE_SHIP_DATE,
			--Start: Changes by Ankit N on 2/19 for reversing timezone conversion for Promise Date
   			--OA.PROMISE_DATE  AS  CUSTOMER_AGRRED_DATE,                             
			--OA.X_PROMISE_DATE_CUST_TZ  AS FINAL_CUSTOMER_AGREED_DATE,    
			--MODIFIED BY ANKIT, BUG NO. 144843
   			to_utc_timestamp(OA.PROMISE_DATE, 'America/Los_Angeles') AS  CUSTOMER_AGRRED_DATE,
			to_utc_timestamp(OA.X_PROMISE_DATE_CUST_TZ, 'America/Los_Angeles') AS FINAL_CUSTOMER_AGREED_DATE,
			--MODIFIED BY ANKIT, BUG NO. 144843
			--End: Changes by Ankit N on 2/19
			OA.SHIP_CONFIRM_DATE_PST SHIP_CONFIRM_DATE,
			OA.APPOINTMENT_PICKUP_LOCAL PICKUP_APPOINTMENT,
			OA.APPOINTMENT_PICKUP_PST,
			CASE 
				WHEN C.COUNTRY_CODE = 'US' THEN 
					CASE 
						WHEN (IO.X_TIME_ZONE_GID = C.TIME_ZONE_GID 
							OR IO.X_TIME_ZONE_GID IN ('-', 'NULL', '') 
							OR C.TIME_ZONE_GID IN ('-', 'NULL', ''))
						THEN OA.APPOINTMENT_PICKUP_LOCAL
						ELSE to_timestamp(
								from_utc_timestamp(
									to_utc_timestamp(
										CAST(OA.APPOINTMENT_PICKUP_LOCAL AS TIMESTAMP),
										COALESCE(NULLIF(IO.X_TIME_ZONE_GID, '-'), 'UTC')
									),
									COALESCE(NULLIF(C.TIME_ZONE_GID, '-'), 'UTC')
								)
							)
					END
				ELSE OA.APPOINTMENT_PICKUP_LOCAL
			END AS DEST_PICKUP_APPOINTMENT,
			OA.APPOINTMENT_DELIVERY_LOCAL DELIVERY_APPOINTMENT,
			OS.STATUS_CODE STATUS,
			DECODE(OS.STATUS_CODE,
       'CANCELLED', OA.REASON_CODE,'CANCELED',  OA.REASON_CODE,NULL) CANCEL_REASON_CODE,
			OA.HIST_CREATED_USER USERNAME,
			LEAST(CAST(COALESCE(WC_214_TMP.DATE_214, OA.X1_STATUS_DATE_WEB_LOCAL, OA.X1_STATUS_DATE_214_LOCAL) AS TIMESTAMP),
					CAST(COALESCE(WC_214_TMP.DATE_214, OA.X1_STATUS_DATE_214_LOCAL, OA.X1_STATUS_DATE_WEB_LOCAL) AS TIMESTAMP)
			) AS LOCAL_ARRIVAL_DATE_214,
			C.CITY_CODE SHIP_TO_CITY,
			C.STATEPROVINCE_CODE SHIP_TO_STATE,
			C.POSTAL_CODE ZIP_CODE,
			C.PARTY_SITE_NUMBER PARTY_SITE_NUMBER,
			C.DISTRIBUTION_CHANNEL_CODE SALES_CHANNEL_CODE,
			C.DSD DTS_FLAG,
			DECODE(C.DSD, 'Y', DECODE(C.DISTRIBUTION_CHANNEL_CODE, 'CLUB', 'DTS Club', 'DTS Non-Club'), 'DC') DTS_DC_CLASSIFICATION,
			OA.CARRIER_NAME CARRIER_NAME,
			OA.CREATED_ON_DT ORDER_CREATION_DATE,
			OA.X_CREATED_ON_DT_CUST_TZ DEST_ORDER_CREATION_DATE,
			OA.SHIPMENT_GID,
			HOS.STATUS_CODE ORDER_HEADER_STATUS,
			I.X_BOTTLE_SIZE BOTTLE_SIZE,
			I.X_PACK_SIZE PACKAGE_SIZE,
			I.X_WATER_TYPE WATER_TYPE,
			I.X_WATER_TYPE2 WATERTYPE,
			I.X_WATER_TYPE_SC WATER_TYPE_SC,
			I.X_PRODUCT PRODUCT,
			I.X_PRODUCT_FAMILY PRODUCT_FAMILY,
			I.X_PRODUCT_CATEGORY PRODUCT_CATEGORY,
			I.X_PRODUCT_CATEGORY_SC PRODUCT_CATEGORY_SC,
			I.X_FG_SALES_CATGORY SALES_CATEGORY,
			OA.HIST_COMMENTS REASON_COMMENTS,
			OA.LOADTYPE LOAD_TYPE,
			OA.OR_TRANSPORT_MODE_GID TRANSPORT_MODE_GID,
			CASE WHEN OA.SH_TRANSPORT_MODE_GID = 'NBL.DROP' THEN 'DROP' ELSE 'NOT-DROP' END SHIPMENT_DROP,
			OA.EARLY_DELIVERY_DATE_LOCAL EARLY_DELIVERY_DATE,
			OA.LATE_DELIVERY_DATE_LOCAL LATE_DELIVERY_DATE,
			CASE WHEN OA.EARLY_DELIVERY_DATE_LOCAL <> OA.LATE_DELIVERY_DATE_LOCAL THEN OA.EARLY_DELIVERY_DATE_LOCAL ELSE NULL END DELIVERY_WINDOW_START,
			CASE WHEN OA.EARLY_DELIVERY_DATE_LOCAL <> OA.LATE_DELIVERY_DATE_LOCAL THEN OA.LATE_DELIVERY_DATE_LOCAL ELSE NULL END DELIVERY_WINDOW_END,
			1 CHANGE_COUNTER,
			IO.X_ROLLUP_ORGANIZATION_CODE ROLLUP_ORGANIZATION_CODE,
			IO.X_SUPPLY_CHAIN_REGION SUPPLY_CHAIN_REGION,
			DECODE(OU.ORG_UNIT_NAME, 'Embotelladora Niagara de Mexico BU', 'NBL/MX', 'NBL') AS DOMAIN_NAME,
			C.COUNTRY_CODE SHIP_TO_COUNTRY,
			CAST(NVL(DECODE(C.CUST_ATTRIBUTE6, '-', 60, 'Unspecified', 60,C.CUST_ATTRIBUTE6), 60) AS INT) BUFFER_TIME_MINUTES,
			CAST(NVL(DECODE(C.SITE_USE_ATTRIBUTE18, '-', 7, 'Unspecified', 7,C.SITE_USE_ATTRIBUTE18), 60) AS INT) CONTRACTUAL_LEAD_TIME,
			C.NEW_ORGANIC_CUSTOMER CS_TEAM,
			C.CUSTOMER_REGION CUSTOMER_REGION,
			C.LOCATION_GID CUSTOMER_LOCATION_GID,
			IO.X_SHIPPING_REGION SHIPPING_REGION,
			C.SITE_USE_ATTRIBUTE3 CS_REPS,
			CAST(NULL AS STRING) AS SCHEDULED_FLAG,
			CAST(NULL AS STRING) AS STATUS_TYPE_GID,
			OA.STATUS_VALUE_GID,
			OA.OR_RATE_OFFERING_GID,
			OA.OR_INDICATOR ORDER_RELEASE_INDICATOR,
			S.NIAGARA_SALES_REGION,
			NVL(OA.OR_RATE_GEO_GID, '-') RATE_GEO_GID,
			NVL(OA.X_LANE_GID, '-') X_LANE_GID,
			NVL(OA.OR_TRANSPORT_MODE_GID, '-') OR_TRANSPORT_MODE_GID,
			NVL(DPCM.FINAL_MILES, 0) FINAL_MILES,
			OA.OR_PENDING_REASON,
			OA.SK_X_INVENTORY_PRODUCT_ID,
			OA.SH_RATE_OFFERING_GID,
			OA.SH_RATE_GEO_GID,
			OA.CANCELLED_QTY CANCELLED_QUANTITY,
			(UOMD.CONVERSION_RATE_TO_24PK * OA.CANCELLED_QTY) CANCELLED_24PKEQCASES,
			(UOMD.CONVERSION_RATE_TO_CASE * OA.CANCELLED_QTY) CANCELLED_CASES,
			(UOMD.CONVERSION_RATE_TO_PALLET * OA.CANCELLED_QTY) CANCELLED_PALLETS,
			OA.X_LANE_GID SH_X_LANE_GID,
			OA.SH_TRANSPORT_MODE_GID SH_TRANSPORT_MODE_GID,
			OA.DEPLOYMENT_FLAG SH_DEPLOYMENT_FLAG,
			OA.SCHEDULE_INSERT_DATE,
			OA.SCHEDULE_INSERT_DATE_PST,
			OA.SCHEDULE_INSERT_DATE_LOCAL,
			CAST(OA.SCHEDULE_UPDATE_DATE AS DATE) AS SCHEDULE_UPDATE_DATE,
			CAST(OA.SCHEDULE_UPDATE_DATE_PST AS DATE)  AS SCHEDULE_UPDATE_DATE_PST,
			CAST(OA.SCHEDULE_UPDATE_DATE_LOCAL AS DATE)  AS SCHEDULE_UPDATE_DATE_LOCAL,
			OA.SCHEDULE_UPDATE_USER,
			OA.CHECK_IN_TIME,
			OA.CHECK_OUT_TIME,
			OA.CHECK_IN_TIME_LOCAL,
			OA.CHECK_OUT_TIME_LOCAL,
			C.ADDRESS,
			OA.DOCK_DOOR,
			OA.DOCK_DOOR_ALLOCATION,
			OA.STARTDATETIME,
			OA.FIRST_LOAD_TIME,
			OA.LAST_LOAD_TIME,
			OA.CLOSE_TRAILER,
			OA.CLOSE_TRAILER_LOCAL,
			I.X_FG_BRAND FG_BRAND,
			CAST(NULL AS STRING) AS DEF_ORG,
			OA.DEF_ROLLUP_ORG_CODE DEF_ROLLUP_ORG_CODE,
			OA.OR_INSERT_DATE_LOCAL ORDER_RELEASE_INSERT_DATE,
			OA.OR_INSERT_DATE_PST OR_INSERT_DATE_PST,
			OA.DEST_ACTUAL_DEPARTURE_LOCAL LOCAL_ACTUAL_DEPARTURE,
			SHIP_TO.ORGANIZATION_CODE SHIP_TO_ORG,
			SHIP_TO.X_ROLLUP_ORGANIZATION_CODE SHIP_TO_ROLLUP_ORG,
			SHIP_TO.X_SUPPLY_CHAIN_REGION SHIP_TO_SC_REGION,
			OT.XACT_SUBTYPE_CODE ORDER_TYPE_CODE,
			CAST(NULL AS STRING) AS LAST_RESCHEDULE_REASON_CODE,
			REQ_WEEK.MCAL_YEAR REQUEST_DT_YEAR,
			REQ_WEEK.MCAL_WEEK REQUEST_DT_WEEK,
			I.SK_INV_MASTER_ITEMS_ID SK_ITEM_ID,
			OA.X_SHIPMENT_PRIORITY_CODE,
			OA.SK_OSP_ID AS OSP,
			OA.SK_MOSP_ID AS MOSP,
			OA.ORG_REQUEST_DATE,
			OA.OR_USER_DEFINED3_ICON_GID OR_USER_DEFINED1_ICON_GID,
			C.TIME_ZONE_GID,
			OA.X_ORDERED_ITEM X_ORDERED_ITEM,
			OA.APPOINTMENT_DELIVERY_PST,
			CASE WHEN OA.EARLY_DELIVERY_DATE_PST <> OA.LATE_DELIVERY_DATE_PST THEN OA.EARLY_DELIVERY_DATE_PST ELSE NULL END DELIVERY_WINDOW_START_LOCAL,
			CASE WHEN OA.EARLY_DELIVERY_DATE_PST <> OA.LATE_DELIVERY_DATE_PST THEN OA.LATE_DELIVERY_DATE_PST ELSE NULL END DELIVERY_WINDOW_END_LOCAL,
			OA.SUPPLY_CHAIN_LOADTYPE,
			OA.X_CREATED_ON_DT_SRC_TZ,
			OA.SHIP_CONFIRM_DATE_LOCAL,
			I.X_CUSTOMER_ITEM_NUMBER CUSTOMER_ITEM,
			I.X_FG_BUSINESS_UNIT FG_BUSINESS_UNIT,
			I.X_FG_FLAVOR FG_FLAVOR,
			I.X_WATER_TYPE PRODUCT_TYPE,
			OA.OLH_DEMAND_CLASS_CODE AS DEMAND_CLASS_CODE,
			OA.ATTRIBUTE13 AOP_RESULT,
			OA.ATTRIBUTE14 GENPLAN_RESULT,
			OA.SH_USER_DEFINED1_ICON_GID,
			OA.SK_X_DESTINATION_ORGANIZATION_ID AS SK_X_DESTINATION_ORGANIZATION_ID,
			OA.SK_INVENTORY_ORG_ID AS SK_INVENTORY_ORG_ID,
			OA.SK_X_UOM_ID AS SK_X_UOM_ID,
			OA.ORDERED_QUANTITY ORDERED_QTY,
			CASE WHEN OA.SOA_SHIPPED_QUANTITY> OA.SALES_QTY THEN OA.SALES_QTY ELSE OA.SOA_SHIPPED_QUANTITY END SOA_SHIPPED_QUANTITY,
			UOMD.CONVERSION_RATE_TO_CASE*(CASE WHEN OA.SOA_SHIPPED_QUANTITY> OA.SALES_QTY THEN OA.SALES_QTY ELSE OA.SOA_SHIPPED_QUANTITY END) ACTUAL_SOA_SHIPPED_QUANTITY,
			OA.SOA_SHIPMENT_DATE,
			OA.SOA_CHECK_IN_TIME,
			OA.SOA_CHECK_OUT_TIME,
			OA.EQUIPMENT_NUMBER,
			OA.SHIPMENT_PRELOAD,
			OA.SK_X_SHIP_TO_ORG_ID AS SK_X_SHIP_TO_ORG_ID,
			C.ADDRESS_DESCRIPTION,
			OA.OREF_SCHEDULE_UPDATE_DATE,
			OA.OREF_SCHEDULE_INSERT_USER,
			OA.OREF_SCHEDULE_UPDATE_DATE_LOCAL,
			OA.OREF_SCHEDULE_INSERT_DATE_LOCAL,
			OA.OREF_SCHEDULE_UPDATE_DATE_PST,
			OA.OREF_SCHEDULE_INSERT_DATE_PST,
			OA.WITH_DRAW_USER_FLAG,
			OA.X_FINAL_REQUEST_DATE_PST FINAL_REQUEST_DATE_PST,
			CASE 
				WHEN OA.X_OLA_ATTRIBUTE16 = 'MTO' THEN 1 
				ELSE 0 
			END MTO_FLG,
			(UOMD.CONVERSION_RATE_TO_CASE * OA.ORDERED_QUANTITY) ORDERED_QTY_CASES,
			C.SITE_USE_ATTRIBUTE4 CS_SCHEDULER,
			FIRST_VALUE(CASE 
							WHEN OA.REASON_CODE IN (
								'03-NIAGARA-OVERSOLD',
								'06-NIAGARA-INVENTORYINACCURACY',
								'09-NIAGARA-PLANNING',
								'10-NIAGARA-MANUFACTURING DELAY',
								'12-NIAGARA-RAW MATERIAL ISSUE',
								'16-NIAGARA - FRCST VARIATIONS',
								'26-SPRIG-WATER-CONSTRAINTS')
							THEN OA.REASON_CODE
							ELSE NULL
						END) OVER ( PARTITION BY OA.SALES_ORDER_NUM, CONCAT(OA.SALES_ORDER_ITEM, '.', OA.X_SHIPMENT_NUMBER)
						ORDER BY 
							CASE 
								WHEN OA.REASON_CODE IN (
									'03-NIAGARA-OVERSOLD',
									'06-NIAGARA-INVENTORYINACCURACY',
									'09-NIAGARA-PLANNING',
									'10-NIAGARA-MANUFACTURING DELAY',
									'12-NIAGARA-RAW MATERIAL ISSUE',
									'16-NIAGARA - FRCST VARIATIONS',
									'26-SPRIG-WATER-CONSTRAINTS') 
								THEN 0 
								ELSE 1 
							END,
							OA.HIST_CREATION_DATE ASC
						ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
			) AS FIRST_REASON_CODE,---no change 142300
			FIRST_VALUE(CASE 
						WHEN OS.STATUS_CODE in ('CANCELLED','CANCELED') AND OA.REASON_CODE IN (
								'03-NIAGARA-OVERSOLD',
								'06-NIAGARA-INVENTORYINACCURACY',
								'09-NIAGARA-PLANNING',
								'10-NIAGARA-MANUFACTURING DELAY',
								'12-NIAGARA-RAW MATERIAL ISSUE',
								'16-NIAGARA - FRCST VARIATIONS',
								'26-SPRING-WATER-CONSTRAINTS')
						THEN OA.REASON_CODE
						ELSE NULL
					END) OVER (PARTITION BY OA.SALES_ORDER_NUM, CONCAT(OA.SALES_ORDER_ITEM, '.', OA.X_SHIPMENT_NUMBER)
					ORDER BY 
						CASE 
							WHEN OS.STATUS_CODE  in ('CANCELLED','CANCELED') AND OA.REASON_CODE IN (
									'03-NIAGARA-OVERSOLD',
									'06-NIAGARA-INVENTORYINACCURACY',
									'09-NIAGARA-PLANNING',
									'10-NIAGARA-MANUFACTURING DELAY',
									'12-NIAGARA-RAW MATERIAL ISSUE',
									'16-NIAGARA - FRCST VARIATIONS',
									'26-SPRING-WATER-CONSTRAINTS')
							THEN 0 
							ELSE 1 
						END,
						OA.HIST_CREATION_DATE ASC
					ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
			) AS FIRST_CANCELLED_REASON_CODE,
			OA.EARLY_PICKUP_DATE_LOCAL AS EARLY_PICKUP_DATE,
			OA.ORDERED_ON_DT,
			OA.OR_TRANSPORT_MODE_GID AS OR_TRANSPORTATION_MODE_GID
		FROM ( --inner join 142300
		select OA.*, OHA.*,OHA.source_App_id source_app_id1 from NBL_FUSION_DW.FACT_SALES_ORDER_LINE OA
		INNER JOIN  NBL_FUSION_DW.FACT_ORDER_HISTORY_ACTIVITY OHA
			ON 	OA.FULFILL_ORDER_LINE_ID=OHA.REFERENCE_LINE_ID
			WHERE OA.SOURCE_APP_ID <> 18
			UNION ALL
		select OA.*,OHA.* ,OHA.source_App_id source_app_id1  from NBL_FUSION_DW.FACT_SALES_ORDER_LINE OA
		INNER JOIN  NBL_FUSION_DW.FACT_ORDER_HISTORY_ACTIVITY OHA
			ON OA.PK_SALES_ORDER_LINE_ID = OHA.LINE_ID 
			WHERE OA.SOURCE_APP_ID = 18
		) OA
		LEFT JOIN NBL_FUSION_DW.DIM_UNIT_OF_MEASURE_D UOMD
			ON OA.SK_X_UOM_ID = UOMD.SK_UNIT_OF_MEASURE_D_ID
		LEFT JOIN NBL_FUSION_DW.DIM_OM_LINE_STATUS OS
			ON OA.SK_X_ORDER_LINE_STATUS_ID = OS.SK_LINE_STATUS_ID
		LEFT JOIN NBL_FUSION_DW.DIM_OM_STATUS HOS
			ON OA.SK_HEADER_STATUS_ID = HOS.SK_STATUS_ID
		LEFT JOIN NBL_FUSION_DW.DIM_INV_MASTER_ITEMS I
			ON OA.SK_X_INVENTORY_PRODUCT_ID = I.SK_INV_MASTER_ITEMS_ID
		LEFT JOIN NBL_FUSION_DW.DIM_HR_ORG IO
			ON OA.SK_INVENTORY_ORG_ID = IO.SK_ORG_ID
		LEFT JOIN NBL_FUSION_DW.DIM_HR_ORG OU
			ON IO.X_OPERATING_UNIT = OU.ORGANIZATION_ID
		LEFT JOIN NBL_FUSION_DW.DIM_CUST_SITE_USE C
			ON OA.SK_X_SHIP_TO_ORG_ID = C.SK_CUST_SITE_USE_ID
		LEFT JOIN NBL_FUSION_DW.DIM_SALES_STATE S
			ON OA.SK_X_SALES_STATE_ID = S.SK_SALES_STATE_ID
		LEFT JOIN NBL_FUSION_DW.DIM_HR_ORG SHIP_TO
			ON OA.SK_X_DESTINATION_ORGANIZATION_ID = SHIP_TO.SK_ORG_ID
		LEFT JOIN NBL_FUSION_DW.DIM_OM_TRANSACTION_TYPE OT
			ON OA.SK_XACT_TYPE_ID = OT.SK_OM_TRANSACTION_TYPE_ID
			/*LEFT JOIN NBLBI_FND_COMMON.DIM_FND_LOOKUP_VALUES FND
			ON OA.FREIGHT_TERMS_ID = FND.LOOKUP_CODE
			AND FND.LOOKUP_TYPE = 'WSH_FREIGHT_CHARGE_TERMS'*/
		--In fusion freight terms are in msc_Sr_lookup_values ,hence using the table directly
		LEFT JOIN nblbi_fusion_fnd_common.MSC_SR_LOOKUP_VALUES_TL FND
		ON FND.LOOKUPCODE = OA.FREIGHT_TERMS_ID
		AND FND.LOOKUPTYPE = 'WSH_FREIGHT_CHARGE_TERMS'
		AND FND.LANGUAGE = 'US'
		LEFT JOIN NBL_FUSION_DW.DIM_MCAL_PLANNING_WEEK REQ_WEEK
			ON OA.SK_X_REQUEST_DT_PLNG_WK_ID = REQ_WEEK.ROW_WID 
		LEFT JOIN NBL_FUSION_DW.DIM_PLANT_TO_CUSTOMER_MILES DPCM
			ON IO.ORGANIZATION_CODE = DPCM.PLANT
			AND C.POSTAL_CODE = DPCM.DEST_ZIPCODE
		LEFT JOIN NBLBI_FILES.WC_214_FIX_BY_DELIVERY_ID_TMP WC_214_TMP
			ON OA.DELIVERY_ID = WC_214_TMP.DELIVERY_ID  --- OA.DELIVERY_ID is null
			WHERE OA.BOOKING_FLG = 'Y'  --AND OA.SALES_ORDER_NUM  IN ('19963107','41140946') 
			AND OA.SALES_ORDER_NUM IN (SELECT SALES_ORDER_NUM AS ORDER_NUMBER 
										FROM (SELECT SALES_ORDER_NUM,'FACT_SALES_ORDER_LINE' X_CUSTOM ,CURRENT_DATE() AS W_INSERT_DT
												FROM NBL_FUSION_DW.FACT_SALES_ORDER_LINE
												
												UNION ALL
												SELECT ORDER_NUMBER,'FACT_ORDER_HISTORY_ACTIVITY' X_CUSTOM ,CURRENT_DATE() AS W_INSERT_DT
												FROM NBL_FUSION_DW.FACT_ORDER_HISTORY_ACTIVITY
												
											)
										)
			AND OA.X_LINE_CATEGORY_CODE <> 'RETURN'
			AND HOS.STATUS_CODE <> 'ENTERED'
			AND OT.XACT_SUBTYPE_CODE IN ('MX_DTS', 'MX_WAREHOUSE XFER', 'BOP SHIPMENT','DTS', 'STANDARD', 'WAREHOUSE XFER', 'MX_STANDARD')
										) A
									) B						 
							) S_VIEW
		LEFT OUTER JOIN NBL_FUSION_DW.DIM_MCAL_PLANNING_WEEK PLW
			ON TO_DATE(S_VIEW.ORIGINAL_REQUEST_DATE) BETWEEN PLW.MCAL_WEEK_START_DT AND PLW.MCAL_WEEK_END_DT )  S_VIEW1) S_VIEW2
			) S_VIEW3
          LEFT OUTER JOIN NBL_FUSION_DW.DIM_MCAL_PLANNING_WEEK PROM_WEEK 
			ON to_date(S_VIEW3.FINAL_CUSTOMER_AGREED_TO_DATE) BETWEEN PROM_WEEK.MCAL_WEEK_START_DT AND PROM_WEEK.MCAL_WEEK_END_DT
)
SELECT * FROM FACT_SERVICE_METRICS
"""
# -------------------------------------------------------------------------------

# Execute the SQL exactly as-is
df = spark.sql(sql_text)
# display(df)

# Write output to Delta table (overwrite mode)
TARGET = "NBL_FUSION_DW.FACT_SERVICE_METRICS"

df.write \
  .format("delta") \
  .mode("overwrite") \
  .saveAsTable(TARGET)

# print(f"SUCCESS: Written to {TARGET}. Row count (first 1 row probe): {df.limit(1).count()}")
