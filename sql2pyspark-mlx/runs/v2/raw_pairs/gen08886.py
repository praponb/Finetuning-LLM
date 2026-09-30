from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .groupBy("session_id")
    .agg(F.count(F.when(F.col("page") == "/cart", 1)).alias("cart_count"))
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("st"), F.col("s.id") == F.col("st.session_id"), "left")
    .filter(F.col("st.cart_count") > 6)
    .select(F.col("s.started_at"), F.col("s.country"), F.col("st.cart_count"))
)
