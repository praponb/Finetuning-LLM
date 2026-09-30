from pyspark.sql import functions as F

base = (
    df_web_events
    .filter(F.col("page").isNotNull() & F.col("page").isin(["/search", "/checkout"]))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.round("duration_sec", 1).alias("duration_sec_rounded"),
        F.when(F.col("event_type").isin(["purchase", "search"]), "Tier 1").otherwise("Other").alias("event_type_group"),
    )
)

result = (
    base
    .alias("a")
    .join(df_sessions.alias("s"), F.col("a.session_id") == F.col("s.id"), "left")
    .select("a.*", F.col("s.country"), F.col("s.browser"))
)
