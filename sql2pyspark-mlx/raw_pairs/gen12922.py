from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("s.plan"),
        F.col("s.status"),
        F.col("c.city"),
        F.col("s.monthly_fee").cast("int").alias("monthly_fee_int"),
    )
)
