from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("s.device"),
        F.col("c.signup_date"),
        F.col("c.credit_limit"),
    )
    .orderBy("credit_limit", "id")
    .limit(3)
)
