from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("s.end_date"),
        F.col("s.status"),
        F.col("s.plan"),
        F.col("c.name"),
        F.col("c.age"),
        F.col("c.city"),
    )
    .orderBy(F.col("status").desc())
)
