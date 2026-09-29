from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel", "customer_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("channel") == "web", 1).otherwise(0))) / F.count("*")), 1).alias("pct_web"),
        F.count("*").alias("total_count"),
    )
)
