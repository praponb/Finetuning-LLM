from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "left")
    .select(
        F.col("we.session_id"),
        F.col("s.customer_id"),
        F.col("s.started_at"),
        F.col("s.country"),
        F.coalesce(F.col("we.page"), F.lit("N/A")).alias("page_filled"),
    )
    .orderBy("country")
)
