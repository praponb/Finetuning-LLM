from pyspark.sql import functions as F

not_flagged = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") <= F.col("s.started_at")), "left_semi")
)

result = (
    not_flagged
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "country",
        F.when(F.col("browser").rlike("^(Chrome|Edge)$"), "PRIORITY").when(F.col("browser").rlike("^(Safari|Firefox)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("browser_zone"),
    )
)
