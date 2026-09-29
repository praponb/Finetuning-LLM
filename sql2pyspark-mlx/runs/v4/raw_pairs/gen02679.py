from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("s.monthly_fee") >= 190)
    .select(
        F.col("s.id"),
        F.col("s.end_date"),
        F.col("c.segment"),
        F.upper(F.col("s.status")).alias("status_upper"),
    )
)
