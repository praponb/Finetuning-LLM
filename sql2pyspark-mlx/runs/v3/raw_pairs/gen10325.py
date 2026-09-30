from pyspark.sql import functions as F

filtered_orders = df_orders.filter(F.col("discount") <= 0.13)

agg = (
    filtered_orders
    .groupBy("status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("channel") == "web", 1).otherwise(0))) / F.count("*")), 1).alias("pct_web"),
    )
)

result = (
    agg
    .select("status", "pct_web")
    .orderBy(F.col("status").desc())
)
