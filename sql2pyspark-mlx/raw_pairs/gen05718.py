from pyspark.sql import functions as F

result = (
    df_subscriptions
    .alias("a")
    .join(df_customers.alias("dim"), F.col("a.customer_id") == F.col("dim.id"), "inner")
    .cube(F.col("dim.country").alias("country"), F.col("a.status").alias("status"))
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("a.monthly_fee")).alias("max_monthly_fee"),
    )
    .orderBy(F.col("cnt").desc_nulls_last())
)
