from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("s.customer_id") == 39)
    .select(
        F.col("s.id"),
        F.col("s.device"),
        F.col("c.name"),
        (F.col("c.credit_limit") - F.col("c.age")).alias("credit_limit_minus_age"),
    )
)
