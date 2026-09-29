from pyspark.sql import functions as F

recent_orders = (
    df_orders
    .filter(F.col("amount").between(590, 860))
    .withColumn("discount_tier", F.when(F.col("discount") >= 0.3, "High").when(F.col("discount") >= 0.06, "Medium").otherwise("Low"))
)

summary = (
    recent_orders
    .groupBy("status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("channel") == "store", 1).otherwise(0))) / F.count("*")), 1).alias("pct_store"),
    )
)

result = summary.orderBy("status", "pct_store")
