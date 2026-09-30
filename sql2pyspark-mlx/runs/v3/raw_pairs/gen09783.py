from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("c.city"),
        F.col("c.email"),
        F.col("s.start_date"),
        F.col("s.plan"),
        F.col("s.monthly_fee"),
    )
    .orderBy("start_date")
)
