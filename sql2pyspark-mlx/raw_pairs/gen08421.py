from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .filter(F.col("event_type") != "search")
    .groupBy("session_id")
    .agg(
        F.sum(F.when(F.col("page") == "/product", 1).otherwise(0)).alias("product_count"),
        F.sum(F.when(F.col("event_type") == "click", 1).otherwise(0)).alias("click_count"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("st"), F.col("s.id") == F.col("st.session_id"), "left")
    .select(
        F.col("s.started_at"),
        F.col("s.customer_id"),
        F.col("st.product_count"),
        F.col("st.click_count"),
    )
    .orderBy(F.col("customer_id").desc())
)
