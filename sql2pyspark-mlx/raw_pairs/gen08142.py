from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("page").orderBy("web_event_event_time", "id")
w3 = Window.partitionBy("page").orderBy("web_event_event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_sessions
    .filter(F.col("started_at") >= F.to_timestamp(F.lit("2023-04-04 00:00:00")))
    .select(
        "id",
        "device",
        "browser",
        "country",
        F.initcap(F.trim("browser")).alias("browser_title"),
    )
)

not_flagged = (
    filtered
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.session_id") == 27), "left_semi")
)

with_last = (
    not_flagged
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.device", "s.browser", F.col("c.page"))
)

with_latest = (
    with_last
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.device",
        "s.page",
        F.col("c.event_time").alias("web_event_event_time"),
    )
)

not_flagged_sessions = (
    with_latest
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.session_id").isin([5, 39, 40])), "left_anti")
)

gaps = (
    not_flagged_sessions
    .withColumn("new_group_flag", F.when(F.col("web_event_event_time") - F.lag("web_event_event_time").over(w2) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_no", F.sum("new_group_flag").over(w3))

result = (
    sessionized
    .groupBy(
        F.when(F.col("new_group_flag") >= 1, "PREMIUM").otherwise("STANDARD").alias("new_group_flag_tier"),
        "session_no",
    )
    .agg(
        F.count(F.when(F.col("page") == "/checkout", 1)).alias("checkout_count"),
        F.countDistinct("device").alias("distinct_devices"),
        F.size(F.collect_set("page")).alias("num_pages"),
        F.count_if(F.col("device") == "mobile").alias("mobile_events"),
        F.round(F.avg("new_group_flag"), 2).alias("avg_new_group_flag"),
        F.round(F.sum("session_no") / F.nullif(F.count(F.when(F.col("page") == "/product", 1)), F.lit(0)), 2).alias("session_no_per_product"),
    )
)
