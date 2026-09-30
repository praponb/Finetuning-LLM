from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(~F.col("s.device").isin(["mobile", "tablet"]) & (F.col("s.country") != "SG"))
    .select(
        F.col("s.id"),
        F.col("s.started_at"),
        F.col("s.customer_id"),
        F.col("s.country"),
        F.col("c.age"),
        F.initcap(F.trim(F.col("c.segment"))).alias("segment_title"),
    )
    .orderBy("started_at", "id")
    .limit(5)
)
