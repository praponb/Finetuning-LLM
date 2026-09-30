from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("s.monthly_fee").isNull())
    .groupBy(F.col("c.segment"))
    .agg(
        F.sum(F.col("s.monthly_fee")).alias("total_monthly_fee"),
        F.round(F.avg(F.col("s.monthly_fee")), 2).alias("avg_monthly_fee"),
    )
    .filter(F.col("avg_monthly_fee") >= 71)
    .orderBy("segment")
)
