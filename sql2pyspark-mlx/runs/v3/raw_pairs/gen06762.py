from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.credit_limit") < 7300)
    .groupBy(F.col("c.city"))
    .agg(
        F.min(F.col("s.start_date")).alias("min_start_date"),
        F.count(F.col("s.monthly_fee")).alias("monthly_fee_count"),
    )
)
