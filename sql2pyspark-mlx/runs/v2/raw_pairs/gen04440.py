from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.filter(F.col("is_active")).alias("c"), F.col("s.customer_id") == F.col("c.id"), "left_semi")
    .select(F.col("s.id"), F.col("s.country"), F.col("s.started_at"))
    .orderBy(F.col("started_at").desc(), "id")
    .limit(10)
)
