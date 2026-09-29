from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .groupBy("session_id")
    .agg(F.count(F.when(F.col("page") == "/product", 1)).alias("product_count"))
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.browser"),
        F.col("s.customer_id"),
        F.col("ss.product_count"),
    )
    .orderBy(F.col("product_count").desc())
)
