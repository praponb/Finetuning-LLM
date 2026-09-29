from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.filter(F.col("name").isNotNull()).alias("c"), F.col("s.customer_id") == F.col("c.id"), "left_semi")
    .filter(F.col("s.browser") != "Edge")
    .select(
        F.col("s.id"),
        F.col("s.started_at"),
        F.col("s.device"),
        F.col("s.customer_id"),
    )
    .orderBy("id")
    .limit(3)
)
