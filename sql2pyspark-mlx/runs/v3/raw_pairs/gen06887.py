from pyspark.sql import functions as F

filtered_shipments = df_shipments.filter((F.col("order_id") == 29) | (F.col("carrier") == "DHL"))

agg = (
    filtered_shipments
    .groupBy(
        F.when(F.col("cost") >= 83, "high").when(F.col("cost") >= 22, "mid").otherwise("low").alias("cost_bucket"),
        F.month("delivered_date").alias("delivered_month"),
    )
    .agg(
        F.min("shipped_date").alias("min_shipped_date"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
)

result = (
    agg
    .select("cost_bucket", "delivered_month", "min_shipped_date", "status_list")
    .orderBy("cost_bucket")
)
