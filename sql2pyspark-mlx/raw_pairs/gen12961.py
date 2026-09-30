from pyspark.sql import functions as F

result = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (~F.col("c.event_type").isin(["click", "add_to_cart"])), "left_anti")
)
