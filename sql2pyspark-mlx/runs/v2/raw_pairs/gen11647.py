from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .filter(F.col("session_id").isin([10, 29]))
    .groupBy("session_id")
    .agg(
        F.sum(F.when(F.col("page") == "/cart", 1).otherwise(0)).alias("cart_count"),
        F.min("duration_sec").alias("min_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("st"), F.col("s.id") == F.col("st.session_id"), "left")
    .filter(F.col("st.cart_count") >= 65)
    .select(
        F.col("s.id"),
        F.col("s.browser"),
        F.col("s.customer_id"),
        F.col("st.cart_count"),
        F.col("st.min_duration_sec"),
    )
    .orderBy("min_duration_sec")
)
