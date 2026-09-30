from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("web_event_page").orderBy(F.col("months_since_started_at").desc(), "id")

with_latest = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.device",
        "s.browser",
        "s.started_at",
        F.col("c.page").alias("web_event_page"),
    )
)

filtered = (
    with_latest
    .filter(F.col("browser").isNotNull() & (~F.col("web_event_page").eqNullSafe(F.lit("/cart"))))
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "web_event_page",
        (F.datediff(F.current_date(), "started_at") / 30).alias("months_since_started_at"),
    )
)

latest_match = (
    filtered
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_time"), F.col("c.page"))
)

deduped = (
    latest_match
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

cleaned = (
    deduped
    .filter(F.col("device").isin(["mobile", "desktop"]))
    .select(
        "id",
        "device",
        "started_at",
        "web_event_page",
        "event_time",
        F.coalesce("web_event_page", F.lit("Unknown")).alias("web_event_page_filled"),
    )
)

summary = (
    cleaned
    .groupBy(F.date_trunc("WEEK", "event_time").alias("week_start"))
    .agg(
        F.max("event_time").alias("last_event_time"),
        F.min("started_at").alias("started_at"),
        F.max("event_time").alias("last_seen"),
        F.count_if(F.col("device") == "mobile").alias("mobile_events"),
    )
    .filter(F.col("mobile_events") < 1000)
)

result = (
    summary
    .select("week_start", "last_event_time", "started_at", "last_seen")
    .orderBy(F.col("last_seen").desc_nulls_first())
)
