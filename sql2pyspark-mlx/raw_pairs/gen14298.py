from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc(), "id")
w2 = Window.partitionBy("session_id", "event_type")
w3 = Window.partitionBy("session_id").orderBy("latest_rank", "id")
w_running = Window.orderBy("next_duration_sec")
w5 = Window.partitionBy("event_type").orderBy(F.col("next_duration_sec").asc_nulls_first())
w_running_2 = Window.partitionBy("event_type")
w_recent = Window.partitionBy("event_type").orderBy("session_id_min_duration_sec")
w8 = Window.orderBy(F.col("event_type_avg_next_duration_sec").desc())

valid = (
    df_web_events
    .filter(F.col("event_time") >= F.lit("2023-04-08").cast("date"))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "duration_sec",
        F.initcap(F.trim("page")).alias("page_title"),
        F.initcap(F.trim("page")).alias("page_title_2"),
        F.coalesce("page", F.lit("OTHER")).alias("page_clean"),
    )
)

deduped = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

scored = (
    deduped
    .filter(F.col("duration_sec") >= 446)
    .select(
        "id",
        "event_type",
        "duration_sec",
        "page_title",
        "page_title_2",
        F.min("duration_sec").over(w2).alias("session_id_min_duration_sec"),
        F.lead("duration_sec", 1).over(w3).alias("next_duration_sec"),
    )
)

kept_keys = (
    scored
    .select("id", "event_type")
    .intersect(
        df_web_events
        .filter(F.to_date("event_time") >= "2023-06-01")
        .select("id", "event_type")
    )
)

verified = (
    scored
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.event_type") == F.col("k.event_type")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .select(
        "*",
        F.cume_dist().over(w_running).alias("next_duration_sec_cume_dist"),
        F.rank().over(w5).alias("next_duration_sec_rank"),
        F.avg("next_duration_sec").over(w_running_2).alias("event_type_avg_next_duration_sec"),
        F.dense_rank().over(w_recent).alias("session_id_min_duration_sec_dense_rank"),
        F.stddev_samp("duration_sec").over(w_running_2).alias("event_type_stddev_samp_duration_sec"),
    )
)

scored_web_events = (
    enriched
    .select(
        "id",
        "event_type",
        "page_title_2",
        "session_id_min_duration_sec",
        "next_duration_sec_cume_dist",
        "next_duration_sec_rank",
        "session_id_min_duration_sec_dense_rank",
        "event_type_stddev_samp_duration_sec",
        F.percent_rank().over(w8).alias("event_type_avg_next_duration_sec_percent_rank"),
        F.stddev_samp("event_type_stddev_samp_duration_sec").over(w_running_2).alias("event_type_stddev_samp_event_type_stddev_samp_duration_sec"),
    )
)

filtered = (
    scored_web_events
    .filter(F.col("next_duration_sec_rank") < 3)
    .withColumn("next_duration_sec_cume_dist_rounded", F.round("next_duration_sec_cume_dist", 0))
    .withColumn("clean_event_type", F.trim("event_type"))
)

result = (
    filtered
    .select(
        "id",
        "event_type",
        "page_title_2",
        "session_id_min_duration_sec",
        "event_type_avg_next_duration_sec_percent_rank",
        "event_type_stddev_samp_event_type_stddev_samp_duration_sec",
        "next_duration_sec_cume_dist_rounded",
        "clean_event_type",
    )
    .orderBy(
        F.col("next_duration_sec_cume_dist_rounded").desc_nulls_last(),
        "event_type_avg_next_duration_sec_percent_rank",
    )
)
