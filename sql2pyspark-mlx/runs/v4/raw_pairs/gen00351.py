from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

base = (
    df_sessions
    .filter(~F.col("country").isin(["JP", "SG"]))
    .withColumn("country_filled", F.coalesce("country", F.lit("unassigned")))
    .withColumn("country_filled_2", F.coalesce("country", F.lit("DIRECT")))
)

base_sessions = (
    base
    .filter(F.col("browser").isNotNull() & F.col("device").isin(["desktop", "tablet", "mobile"]))
    .withColumn("browser_title", F.initcap(F.trim("browser")))
)

latest_match = (
    base_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.duration_sec"), F.col("c.page").alias("web_event_page"))
)

with_latest = (
    latest_match
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.page"), F.col("c.event_time"))
)

result = (
    with_latest
    .select(
        "id",
        "device",
        "started_at",
        "country_filled",
        "country_filled_2",
        "web_event_page",
        "event_time",
        F.when(F.col("device").rlike("^(desktop|mobile)$"), "GROUP_A").when(F.col("device").rlike("^(tablet)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("device_zone"),
    )
    .orderBy(F.col("device_zone").desc())
)
