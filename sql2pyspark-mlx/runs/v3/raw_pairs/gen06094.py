from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "right")
    .filter(F.col("s.country").isin(["UK", "US", "DE"]))
    .select(
        F.col("s.device"),
        F.col("s.country"),
        F.col("c.signup_date"),
        F.col("c.credit_limit"),
        F.col("c.country").alias("customer_country"),
    )
)
