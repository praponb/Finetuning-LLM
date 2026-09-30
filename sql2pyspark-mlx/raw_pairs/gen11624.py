from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.segment"))
    .agg(
        F.round(F.avg(F.col("s.monthly_fee")), 2).alias("avg_monthly_fee"),
        F.countDistinct(F.col("s.monthly_fee")).alias("unique_plans"),
    )
    .orderBy(F.col("avg_monthly_fee").desc())
)
