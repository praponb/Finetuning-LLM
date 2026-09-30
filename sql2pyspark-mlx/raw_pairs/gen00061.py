from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.col("s.country").isin(["UK", "SG", "US"]) & F.col("we.page").isin(["/home", "/product", "/checkout"]))
    .select(F.col("s.device"), F.col("s.browser"), F.col("s.country"), F.col("we.page"))
)
