from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.filter(F.col("customer_id").isin([5, 11, 25, 32])).alias("s"), F.col("we.session_id") == F.col("s.id"), "left_semi")
    .filter(F.col("we.event_type").isin(["purchase", "page_view", "add_to_cart"]))
    .select(
        F.col("we.id"),
        F.col("we.session_id"),
        F.col("we.page"),
        F.col("we.event_type"),
    )
)
