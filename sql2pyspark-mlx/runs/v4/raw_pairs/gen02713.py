from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("c.email"),
        F.col("s.start_date"),
        F.col("s.status"),
        F.col("s.end_date"),
    )
    .orderBy("status")
)
