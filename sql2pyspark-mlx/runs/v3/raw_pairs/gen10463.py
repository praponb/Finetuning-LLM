from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isNotNull() & (F.col("store_id").isin([6, 19]) | F.col("store_id").isNotNull()))
    .groupBy("channel")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("channel") == "web", 1).otherwise(0))) / F.count("*")), 1).alias("pct_web"),
    )
    .orderBy(F.col("pct_web").desc())
)
