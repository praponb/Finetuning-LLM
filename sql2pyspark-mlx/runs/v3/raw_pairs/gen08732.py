from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .filter(F.col("page").isin(["/home", "/product", "/search"]))
    .groupBy("session_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0))) / F.count("*")), 1).alias("pct_purchase"),
        F.count("*").alias("num_web_events"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("st"), F.col("s.id") == F.col("st.session_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.browser"),
        F.col("st.pct_purchase"),
        F.col("st.num_web_events"),
    )
    .orderBy(F.col("id").desc())
)
