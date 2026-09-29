from pyspark.sql import functions as F

result = (
    df_subscriptions
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.segment").alias("segment"), F.col("src.status").alias("status"))
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("src.monthly_fee")).alias("max_monthly_fee"),
    )
    .orderBy(F.col("cnt").desc_nulls_last())
)
