from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .groupBy(
        F.when(F.col("s.sqft") >= 16084, "high").when(F.col("s.sqft") >= 6661, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("o.channel") == "partner", 1).otherwise(0))) / F.count("*")), 1).alias("pct_partner"),
        F.round(F.stddev(F.col("o.discount")), 2).alias("stddev_discount"),
    )
    .orderBy("sqft_bucket")
)
