from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .filter(F.col("s.customer_id").isNotNull())
    .join(df_customers.filter(~F.col("is_active")).alias("c"), F.col("s.customer_id") == F.col("c.id"), "left_anti")
    .filter(F.col("s.country").isin(["JP", "DE"]))
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("s.device"),
        F.col("s.customer_id"),
    )
)
