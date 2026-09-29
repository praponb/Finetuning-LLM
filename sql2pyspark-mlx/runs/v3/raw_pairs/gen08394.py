from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("s.status") == "ACTIVE")
    .groupBy(F.col("c.segment"))
    .agg(
        F.round(F.avg(F.col("s.monthly_fee")), 2).alias("avg_monthly_fee"),
        F.max(F.col("s.start_date")).alias("latest_start_date"),
    )
)
