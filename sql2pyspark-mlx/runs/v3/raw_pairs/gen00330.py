from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.city").isin(["Chiang Mai", "Bangkok"]))
    .select(
        F.col("s.id"),
        F.col("s.device"),
        F.col("s.browser"),
        F.col("s.country"),
        F.col("c.signup_date"),
        F.col("c.credit_limit"),
        F.col("c.country").alias("customer_country"),
        F.coalesce(F.col("s.country"), F.lit("N/A")).alias("country_filled"),
    )
)
