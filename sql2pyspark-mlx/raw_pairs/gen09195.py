from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("started_at").desc(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

filtered = (
    df_sessions
    .filter(F.col("device") == "desktop")
    .withColumn("device_title", F.initcap(F.trim("device")))
)

ranked = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

cleaned = (
    ranked
    .filter(F.col("browser").isNotNull())
    .select(
        "id",
        "device",
        "started_at",
        "country",
        "device_title",
        "latest_rank",
        F.when(F.col("browser") == "Firefox", 1).otherwise(0).alias("is_firefox"),
        F.col("latest_rank").cast("double").alias("latest_rank_str"),
    )
)

with_last = (
    cleaned
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.device_title",
        "s.latest_rank_str",
        F.col("c.event_type").alias("web_event_event_type"),
        F.col("c.duration_sec").alias("web_event_duration_sec"),
    )
)

not_flagged = (
    with_last
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.session_id").isin([2, 32])), "left_anti")
)

kept = (
    not_flagged
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.session_id").isin([1, 4, 22, 29])), "left_semi")
)

result = (
    kept
    .cube("web_event_event_type")
    .agg(
        F.max("latest_rank_str").alias("max_latest_rank_str"),
        F.sort_array(F.collect_set("web_event_event_type")).alias("web_event_event_type_list"),
        F.round(F.sum("latest_rank_str") / F.nullif(F.count(F.when(F.col("web_event_event_type") == "search", 1)), F.lit(0)), 2).alias("latest_rank_str_per_search"),
        F.percentile_approx("web_event_duration_sec", 0.5).alias("web_event_duration_sec_p50"),
    )
    .filter(F.col("latest_rank_str_per_search") < 1000000)
)
