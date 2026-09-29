from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .groupBy(F.col("c.country"))
    .agg(
        (F.max(F.col("s.monthly_fee")) - F.min(F.col("s.monthly_fee"))).alias("monthly_fee_range"),
        F.min(F.col("s.start_date")).alias("first_start_date"),
    )
    .orderBy(F.col("monthly_fee_range").desc(), "country")
)
