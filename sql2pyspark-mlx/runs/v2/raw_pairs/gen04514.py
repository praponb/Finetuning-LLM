from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("c.country") == "US")
    .select(
        F.col("c.city"),
        F.col("c.age"),
        F.col("c.signup_date"),
        F.col("s.browser"),
        F.col("s.customer_id"),
        F.last_day(F.col("c.signup_date")).alias("signup_date_month_end"),
    )
    .orderBy("customer_id")
)
