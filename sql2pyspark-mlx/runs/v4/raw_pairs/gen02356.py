from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "left")
    .filter(F.col("s.customer_id").isin([7, 15, 40]))
    .select(
        F.col("c.email"),
        F.col("c.is_active"),
        F.col("c.credit_limit"),
        F.col("s.browser"),
        F.col("s.device"),
    )
)
