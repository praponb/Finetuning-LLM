from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("s.browser").isin(["Safari", "Firefox", "Chrome"]))
    .select(
        F.col("s.id"),
        F.col("s.browser"),
        F.col("c.city"),
        F.col("c.signup_date"),
        F.col("c.name"),
    )
    .orderBy("browser")
)
