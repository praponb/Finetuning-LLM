from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.country").isin(["TH", "SG", "DE"]) & F.col("s.customer_id").isNull())
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("s.device"),
        F.col("s.browser"),
        F.col("c.credit_limit"),
    )
)
