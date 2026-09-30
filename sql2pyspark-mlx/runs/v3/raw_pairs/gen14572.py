from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.filter(F.col("browser").isin(["Edge", "Safari"])).alias("s"), F.col("we.session_id") == F.col("s.id"), "left_anti")
    .filter(~F.col("we.event_type").isin(["add_to_cart", "click", "search"]))
    .select(
        F.col("we.id"),
        F.col("we.event_time"),
        F.col("we.event_type"),
        F.col("we.page"),
    )
)
