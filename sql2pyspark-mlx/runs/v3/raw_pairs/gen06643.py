from pyspark.sql import functions as F

valid_shipments = df_shipments.filter((F.col("cost") >= 72) & (F.col("status") != "IN_TRANSIT"))

status_summary = (
    valid_shipments
    .groupBy("status")
    .agg(
        F.max("delivered_date").alias("latest_delivered_date"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
    )
)

result = status_summary.select("status", "latest_delivered_date")
