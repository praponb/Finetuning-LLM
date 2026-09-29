from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.age") <= 39)
    .select(
        F.col("s.id"),
        F.col("s.started_at"),
        F.col("s.browser"),
        F.col("c.credit_limit"),
        F.col("c.email"),
        F.col("c.signup_date"),
    )
    .orderBy("email", "id")
    .limit(5)
)
