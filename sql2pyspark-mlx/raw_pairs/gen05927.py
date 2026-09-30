from pyspark.sql import functions as F

filtered = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select(
        "*",
        F.when(F.col("duration_sec") >= 473, "High").when(F.col("duration_sec") >= 234, "Medium").otherwise("Low").alias("duration_sec_tier"),
    )
)

result = (
    filtered
    .alias("src")
    .join(df_sessions.alias("d"), F.col("src.session_id") == F.col("d.id"), "left")
    .select("src.id", "src.session_id", "src.duration_sec", F.col("d.browser"))
)
