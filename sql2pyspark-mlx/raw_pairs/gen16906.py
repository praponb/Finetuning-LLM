from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.browser"),
        F.col("s.customer_id"),
        F.col("c.credit_limit"),
        F.col("c.segment"),
        F.col("c.is_active"),
    )
    .orderBy("id", "browser")
)
