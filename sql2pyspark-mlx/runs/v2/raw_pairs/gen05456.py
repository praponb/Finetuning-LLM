from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("s.device") == "mobile")
    .select(
        F.col("s.customer_id"),
        F.col("s.browser"),
        F.col("c.email"),
        F.col("c.signup_date"),
        F.col("c.city"),
    )
)
