from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .filter(~F.col("page").isin(["/product", "/checkout", "/cart"]))
    .groupBy("session_id")
    .agg(
        F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart_count"),
        F.max("duration_sec").alias("max_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("st"), F.col("s.id") == F.col("st.session_id"), "inner")
    .filter(F.col("st.add_to_cart_count") > 20)
    .select(
        F.col("s.customer_id"),
        F.col("s.browser"),
        F.col("st.add_to_cart_count"),
        F.col("st.max_duration_sec"),
    )
)
