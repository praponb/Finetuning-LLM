from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "outer")
    .filter(F.col("c.age").between(36, 61))
    .select(
        F.col("s.id"),
        F.col("s.customer_id"),
        F.col("c.name"),
        F.col("c.city"),
        F.col("c.age"),
    )
)
