from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.country"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("we.event_type") == "page_view", 1).otherwise(0))) / F.count("*")), 1).alias("pct_page_view"),
        F.sum(F.when(F.col("we.page") == "/cart", 1).otherwise(0)).alias("cart_count"),
    )
    .orderBy(F.col("pct_page_view").desc(), "country")
)
