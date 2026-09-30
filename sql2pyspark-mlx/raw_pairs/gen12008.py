from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .groupBy(
        F.when(F.col("c.age") >= 73, "high").when(F.col("c.age") >= 41, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(F.max(F.col("s.started_at")).alias("latest_started_at"))
    .orderBy(F.col("age_bucket").desc())
)
