from pyspark.sql import functions as F

recent_shipments = df_shipments.filter(F.col("status") == "RETURNED")

summary = (
    recent_shipments
    .groupBy(
        F.when(F.col("cost") >= 95, "high").when(F.col("cost") >= 14, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.sum(F.when(F.col("carrier") == "Flash", F.col("cost")).otherwise(0)).alias("flash_cost"),
    )
)

result = summary.filter(F.col("flash_cost") > 62)
