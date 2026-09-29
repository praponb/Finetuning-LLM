from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "left")
    .filter(F.col("s.status") == "PAUSED")
    .select(
        F.col("s.end_date"),
        F.col("s.monthly_fee"),
        F.col("s.status"),
        F.col("c.age"),
    )
    .orderBy("monthly_fee", F.col("age").desc())
)
