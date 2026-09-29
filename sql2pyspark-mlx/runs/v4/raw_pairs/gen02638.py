from pyspark.sql import functions as F

base = df_orders.filter(F.col("amount") <= 1270)

orders_stats = (
    base
    .groupBy("channel", "status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("channel") == "web", 1).otherwise(0))) / F.count("*")), 1).alias("pct_web"),
    )
)

result = (
    orders_stats
    .select("channel", "status", "pct_web")
    .orderBy("pct_web")
)
