from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .groupBy("session_id")
    .agg(F.sum(F.when(F.col("page") == "/cart", 1).otherwise(0)).alias("cart_count"))
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("st"), F.col("s.id") == F.col("st.session_id"), "inner")
    .filter(F.col("st.cart_count") > 52)
    .select(F.col("s.started_at"), F.col("s.customer_id"), F.col("st.cart_count"))
    .orderBy("customer_id")
)
