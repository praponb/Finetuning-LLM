from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.monthly_fee"),
        F.col("s.customer_id"),
        F.col("s.end_date"),
        F.col("c.email"),
        F.round((F.datediff(F.col("s.start_date"), F.col("s.end_date")) / 30), 1).alias("months_end_date_to_start_date"),
    )
)
