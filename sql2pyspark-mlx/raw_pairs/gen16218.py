from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .filter(F.col("session_id").isin([31, 36]))
    .groupBy("session_id")
    .agg(
        F.count(F.when(F.col("page") == "/checkout", 1)).alias("checkout_count"),
        F.count(F.when(F.col("page") == "/search", 1)).alias("search_count"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("st"), F.col("s.id") == F.col("st.session_id"), "inner")
    .select(F.col("s.started_at"), F.col("st.checkout_count"), F.col("st.search_count"))
    .orderBy("checkout_count")
)
