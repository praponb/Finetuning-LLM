from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        "order_id",
        "status",
        F.when(F.col("amount") >= 1540, "high").when(F.col("amount") >= 400, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)
