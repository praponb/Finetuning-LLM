from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.col("we.event_type").isin(["add_to_cart", "purchase", "page_view"]))
    .groupBy(F.date_trunc("MONTH", F.col("s.started_at")).alias("started_month"))
    .agg(F.sum(F.when(F.col("we.page") == "/cart", 1).otherwise(0)).alias("cart_count"))
    .filter(F.col("cart_count") >= 80)
)
