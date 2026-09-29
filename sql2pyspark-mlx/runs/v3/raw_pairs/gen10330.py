from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "left")
    .filter(F.col("c.is_active"))
    .groupBy(
        F.when(F.col("s.sqft") >= 15517, "high").when(F.col("s.sqft") >= 6161, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.max(F.col("o.discount")).alias("max_discount"),
        F.min(F.col("o.amount")).alias("min_amount"),
    )
    .filter(F.col("min_amount") > 1350)
)
