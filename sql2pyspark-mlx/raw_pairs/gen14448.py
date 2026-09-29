from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("browser").orderBy("started_at", "id")
w3 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())

with_latest = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.event_time").alias("web_event_event_time"),
        F.col("c.duration_sec"),
    )
)

gaps = (
    with_latest
    .withColumn("is_new_burst", F.when(F.col("started_at") - F.lag("started_at").over(w2) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessions_sessions = gaps.withColumn("burst_id", F.sum("is_new_burst").over(w3))

with_latest_sessions = (
    sessions_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.web_event_event_time")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.browser",
        F.col("c.duration_sec").alias("web_event_duration_sec"),
        F.col("c.page").alias("web_event_page"),
    )
)

kept_keys = (
    with_latest_sessions
    .select("id", "browser")
    .subtract(
        df_sessions
        .filter(F.col("customer_id") == 23)
        .select("id", "browser")
    )
)

verified = (
    with_latest_sessions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.browser") == F.col("k.browser")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.when(F.col("web_event_duration_sec") >= 393, "LARGE").otherwise("SMALL").alias("web_event_duration_sec_tier"),
    )
    .agg(
        F.round(F.sum("web_event_duration_sec") / F.nullif(F.count(F.when(F.col("browser") == "Edge", 1)), F.lit(0)), 2).alias("web_event_duration_sec_per_edge"),
        F.round(F.avg("web_event_duration_sec"), 2).alias("avg_web_event_duration_sec"),
    )
    .filter(F.col("avg_web_event_duration_sec") > 0)
)
