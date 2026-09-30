from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("c.country"),
        F.col("c.name"),
        F.col("c.signup_date"),
        F.col("s.device"),
        F.col("s.country").alias("session_country"),
        F.col("s.started_at"),
        F.length(F.col("s.browser")).alias("browser_length"),
    )
    .orderBy("signup_date")
)
