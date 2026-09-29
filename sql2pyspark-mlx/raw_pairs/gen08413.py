from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("duration_sec_rounded").desc())
w2 = Window.partitionBy("session_id").orderBy(F.col("event_time").desc(), "id")

filtered = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-09-19").cast("date")) & F.col("page").isNotNull())
    .withColumn("duration_sec_rounded", F.round("duration_sec", 2))
    .withColumn("event_time_label", F.date_format("event_time", "yyyy-MM-dd"))
    .withColumn("resolved_page", F.coalesce("page", F.lit("NONE")))
)

latest = (
    filtered
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec_rounded",
        "event_time_label",
        "resolved_page",
    )
)

latest_web_events = (
    latest
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

enriched = (
    latest_web_events
    .alias("src")
    .join(df_sessions.alias("d"), F.col("src.session_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.country"), F.col("d.browser"), F.col("d.device"))
)

with_dims = (
    enriched
    .alias("a")
    .join(df_sessions.alias("s"), F.col("a.session_id") == F.col("s.id"), "inner")
    .select(
        "a.*",
        F.col("s.country").alias("session_country"),
        F.col("s.device").alias("session_device"),
    )
)

result = (
    with_dims
    .groupBy("device")
    .pivot("session_country", ["JP", "DE"])
    .agg(F.sum("duration_sec_rounded"))
    .select("device", F.col("JP").alias("jp"), F.col("DE").alias("de"))
)
