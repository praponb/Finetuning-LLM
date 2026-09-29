from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .filter(F.col("event_type") == "search")
    .groupBy("session_id")
    .agg(
        F.sum(F.when(F.col("page") == "/product", F.col("duration_sec")).otherwise(0)).alias("product_duration_sec"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("st"), F.col("s.id") == F.col("st.session_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.customer_id"),
        F.col("st.product_duration_sec"),
        F.col("st.avg_duration_sec"),
    )
)
