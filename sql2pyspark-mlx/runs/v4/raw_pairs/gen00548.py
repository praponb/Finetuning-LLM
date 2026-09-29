from pyspark.sql import functions as F

result = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_type") == "add_to_cart"), "left_semi")
)
