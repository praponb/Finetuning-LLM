from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "outer")
    .filter(F.col("c.credit_limit") >= 15500)
    .select(
        F.col("s.end_date"),
        F.col("s.customer_id"),
        F.col("c.city"),
        F.col("c.name"),
        F.col("c.age"),
    )
    .orderBy("city")
)
