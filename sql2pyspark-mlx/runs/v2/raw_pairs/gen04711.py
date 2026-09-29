from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "left")
    .filter(F.col("c.is_active"))
    .select(
        F.col("c.segment"),
        F.col("c.signup_date"),
        F.col("c.is_active"),
        F.col("s.customer_id"),
        F.col("s.browser"),
    )
    .orderBy("segment")
)
