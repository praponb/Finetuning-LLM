from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .filter(F.col("event_time") >= "2023-12-01")
    .groupBy("session_id")
    .agg(
        F.sum(F.when(F.col("page") == "/search", 1).otherwise(0)).alias("search_count"),
        F.sum(F.when(F.col("page") == "/checkout", 1).otherwise(0)).alias("checkout_count"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("st"), F.col("s.id") == F.col("st.session_id"), "left")
    .select(
        F.col("s.country"),
        F.col("s.customer_id"),
        F.col("st.search_count"),
        F.col("st.checkout_count"),
    )
    .orderBy(F.col("customer_id").desc())
)
