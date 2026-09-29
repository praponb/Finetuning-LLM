from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "left")
    .groupBy(F.col("s.customer_id"))
    .agg(
        (F.max(F.col("we.duration_sec")) - F.min(F.col("we.duration_sec"))).alias("duration_sec_range"),
        F.count(F.when(F.col("we.page") == "/product", 1)).alias("product_count"),
    )
)
