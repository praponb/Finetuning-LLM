from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.end_date"),
        F.col("s.plan"),
        F.col("s.status"),
        F.col("c.segment"),
    )
    .orderBy("plan", "id")
    .limit(10)
)
