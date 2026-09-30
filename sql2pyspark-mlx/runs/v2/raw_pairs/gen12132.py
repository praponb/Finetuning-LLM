from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left")
    .filter(~F.col("s.device").isin(["tablet", "mobile", "desktop"]))
    .select(
        F.col("s.country"),
        F.col("s.customer_id"),
        F.col("we.event_time"),
        F.col("we.page"),
        F.when(F.col("we.event_type") == "purchase", 1).otherwise(0).alias("is_purchase"),
    )
)
