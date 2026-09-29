from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "right")
    .groupBy(
        F.when(F.col("o.discount") >= 0.36, "high").when(F.col("o.discount") >= 0.19, "mid").otherwise("low").alias("discount_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("s.status") == "LOST", 1).otherwise(0))) / F.count("*")), 1).alias("pct_lost"),
    )
)
