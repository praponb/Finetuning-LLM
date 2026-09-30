from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("s.browser"),
        F.col("s.country"),
        F.col("c.segment"),
        F.col("c.email"),
        F.col("c.signup_date"),
    )
    .orderBy("email")
)
