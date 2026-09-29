from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel", "status")
    .agg(
        F.count("amount").alias("amount_count"),
        F.round(((100 * F.sum(F.when(F.col("channel") == "web", 1).otherwise(0))) / F.count("*")), 1).alias("pct_web"),
        F.avg("discount").alias("avg_discount"),
    )
    .filter(F.col("avg_discount") >= 0.1)
    .drop("avg_discount")
)
