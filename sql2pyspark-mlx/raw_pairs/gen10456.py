from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("s.customer_id") == 11)
    .groupBy(F.col("c.country"))
    .agg(
        F.sum(F.when(F.col("s.status") == "EXPIRED", F.col("s.monthly_fee")).otherwise(0)).alias("expired_monthly_fee"),
        F.max(F.col("s.monthly_fee")).alias("max_monthly_fee"),
    )
    .orderBy("expired_monthly_fee", "country")
    .limit(20)
)
