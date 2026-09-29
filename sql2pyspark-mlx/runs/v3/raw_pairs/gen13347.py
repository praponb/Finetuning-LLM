from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .groupBy(F.col("c.country"))
    .agg(
        F.sum(F.col("s.monthly_fee")).alias("total_monthly_fee"),
        F.round(F.avg(F.col("s.monthly_fee")), 2).alias("avg_monthly_fee"),
    )
    .filter(F.col("total_monthly_fee") >= 580)
    .orderBy("country")
    .limit(50)
)
