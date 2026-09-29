from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isin(["partner", "web"]))
    .groupBy(
        F.when(F.col("amount") >= 1470, "high").when(F.col("amount") >= 770, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "PENDING", 1).otherwise(0)).alias("pending_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
    .filter(F.col("pending_count") > 22)
)
