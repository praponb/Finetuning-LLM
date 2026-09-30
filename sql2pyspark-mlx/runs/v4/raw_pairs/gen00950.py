from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter((F.col("s.country") == "DE") | F.col("s.device").isin(["tablet", "desktop", "mobile"]))
    .select(
        F.col("we.event_type"),
        F.col("s.country"),
        F.col("s.browser"),
        F.col("c.name"),
    )
)
