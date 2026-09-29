from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("s.monthly_fee") < 130)
    .select(
        F.col("c.city"),
        F.col("s.status"),
        F.col("s.customer_id"),
        F.col("s.end_date"),
    )
    .orderBy("city", F.col("customer_id").desc())
)
