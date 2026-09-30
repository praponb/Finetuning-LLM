from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .select(
        F.col("c.country"),
        F.col("s.device"),
        F.col("s.country").alias("session_country"),
    )
    .orderBy(F.col("country").desc())
)
