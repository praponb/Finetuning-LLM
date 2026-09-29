from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.col("s.customer_id").isin([7, 10, 36, 40]))
    .groupBy(F.col("s.country"))
    .agg(
        F.sum(F.when(F.col("we.page") == "/product", F.col("we.duration_sec")).otherwise(0)).alias("product_duration_sec"),
        F.max(F.col("we.duration_sec")).alias("max_duration_sec"),
    )
    .orderBy(F.col("product_duration_sec").desc(), "country")
    .limit(1)
)
