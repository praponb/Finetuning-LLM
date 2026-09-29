from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())

cleaned = (
    df_sessions
    .filter(F.col("browser").isin(["Edge", "Firefox"]) & (F.col("started_at") > F.lit("2023-04-21 12:00:00").cast("timestamp")))
    .select(
        "*",
        F.when(F.col("device").isin(["tablet", "mobile"]), "Tier 1").otherwise("Other").alias("device_group"),
        F.coalesce("browser", F.lit("N/A")).alias("browser_filled"),
    )
)

result = (
    cleaned
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.browser",
        "s.started_at",
        "s.country",
        "s.device_group",
        "s.browser_filled",
        F.col("c.event_type").alias("web_event_event_type"),
        F.col("c.duration_sec"),
    )
)
