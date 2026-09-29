from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.when(F.col("cost") >= 88, "high").when(F.col("cost") >= 34, "mid").otherwise("low").alias("cost_bucket"),
        "order_id",
    )
    .agg(
        F.avg("cost").alias("avg_cost"),
        F.concat_ws(", ", F.sort_array(F.collect_set("carrier"))).alias("carrier_values"),
    )
    .filter(F.col("avg_cost") >= 58)
    .orderBy("cost_bucket")
)
