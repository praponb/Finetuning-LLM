from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isin(["partner", "store"]) & (F.col("discount") < 0.24) & (F.col("order_date") < "2025-05-20"))
    .groupBy("status", "channel")
    .agg(
        F.count("discount").alias("discount_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.max("order_date").alias("last_order_date"),
        F.countDistinct("discount").alias("unique_statuss"),
    )
    .filter(F.col("unique_statuss") < 2)
    .drop("unique_statuss")
    .orderBy("status_values", "status", "channel")
    .limit(1)
)
