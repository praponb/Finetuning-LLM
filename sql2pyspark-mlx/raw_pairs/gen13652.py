from pyspark.sql import functions as F

filtered = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-05-27 05:00:00").cast("timestamp")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        F.initcap(F.trim("device")).alias("device_title"),
        F.coalesce("browser", F.lit("unassigned")).alias("browser_filled"),
    )
)

result = (
    filtered
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") <= F.col("s.started_at")), "left_semi")
)
