from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.sum(F.when(F.col("page") == "/product", 1).otherwise(0)).alias("product_count"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("st"), F.col("s.id") == F.col("st.session_id"), "inner")
    .filter(F.col("st.product_count") >= 42)
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("s.device"),
        F.col("st.avg_duration_sec"),
        F.col("st.product_count"),
    )
    .orderBy(F.col("product_count").desc())
)
