from pyspark.sql import functions as F

result = (
    df_web_events
    .alias("e")
    .join(df_sessions.alias("b"), F.col("e.session_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.country"), F.col("b.device"), F.col("b.browser"))
)
