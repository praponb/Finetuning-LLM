from pyspark.sql import functions as F

recent_shipments = (
    df_shipments
    .filter((F.col("delivered_date") >= "2025-05-01") & F.col("order_id").isNull())
)

agg = (
    recent_shipments
    .groupBy("status")
    .agg(
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
        F.min("delivered_date").alias("first_delivered_date"),
    )
)

result = (
    agg
    .orderBy("first_delivered_date", "status")
    .limit(10)
)
