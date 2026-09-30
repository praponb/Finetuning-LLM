from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("c.city"),
        F.col("c.is_active"),
        F.col("c.credit_limit"),
    )
    .orderBy("country")
)
