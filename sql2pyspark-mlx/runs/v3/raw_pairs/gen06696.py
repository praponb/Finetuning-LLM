from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter((F.col("s.browser") == "Chrome") & F.col("s.customer_id").isNull())
    .select(
        F.col("we.id"),
        F.col("we.session_id"),
        F.col("s.customer_id"),
        F.concat(F.col("s.browser"), F.lit(" "), F.col("s.country")).alias("browser_country"),
    )
    .orderBy("customer_id", "id")
    .limit(50)
)
