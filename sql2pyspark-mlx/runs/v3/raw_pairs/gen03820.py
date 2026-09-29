from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.col("c.credit_limit") > 10700)
    .groupBy(
        F.col("s.region"),
        F.when(F.col("s.sqft") >= 12854, "high").when(F.col("s.sqft") >= 2823, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(F.count(F.col("o.amount")).alias("amount_count"))
    .filter(F.col("amount_count") > 4)
    .orderBy(F.col("amount_count").desc())
)
