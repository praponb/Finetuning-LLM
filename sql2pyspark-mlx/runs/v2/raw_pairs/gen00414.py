from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.device"),
        F.col("c.name"),
        F.col("c.city"),
        F.col("c.credit_limit"),
        F.date_add(F.col("c.signup_date"), 6).alias("signup_date_plus_6d"),
    )
)
