from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "outer")
    .groupBy(F.col("c.city"), F.col("c.is_active"))
    .agg(
        F.countDistinct(F.col("s.country")).alias("unique_countrys"),
        F.min(F.col("s.started_at")).alias("first_started_at"),
    )
    .orderBy("first_started_at", F.col("city").desc())
)
