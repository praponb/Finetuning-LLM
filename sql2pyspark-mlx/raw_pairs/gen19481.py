from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "right")
    .select(
        F.col("s.customer_id"),
        F.col("c.segment"),
        F.col("c.credit_limit"),
        F.when(F.col("s.device") == "tablet", 1).otherwise(0).alias("is_tablet"),
    )
)
