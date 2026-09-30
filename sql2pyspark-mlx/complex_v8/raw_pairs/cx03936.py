from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("device").orderBy(F.col("ms_played_bucket").desc(), F.col("id").desc())

cleaned_plays = (
    df_plays
    .filter(F.col("source") == "search")
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket_2"),
    )
)

scored = (
    cleaned_plays
    .select(
        "id",
        "listener_id",
        "played_at",
        "device",
        "source",
        "ms_played_bucket",
        F.to_date(F.col("played_at")).alias("played_at_date"),
        F.lower(F.regexp_replace(F.trim(F.col("source")), " ", "_")).alias("source_slug"),
    )
)

unique_rows = (
    scored
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(F.max("ms_played").alias("max_play_ms_played"))
)

with_stats = (
    unique_rows.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select("s.*", "p.max_play_ms_played")
)

rollup_base = (
    with_stats
    .groupBy("device", "listener_id", "source")
    .agg(
        F.round(F.sum("max_play_ms_played") / F.nullif(F.sum("ms_played_bucket"), F.lit(0)), 3).alias("max_play_ms_played_to_ms_played_bucket_ratio"),
        F.array_sort(F.collect_set("source")).alias("source_list"),
        F.countDistinct("source").alias("distinct_sources"),
        F.max("max_play_ms_played").alias("max_max_play_ms_played_check"),
    )
    .filter(F.col("max_max_play_ms_played_check") > 123500)
    .drop("max_max_play_ms_played_check")
)

with_parent_listener = (
    rollup_base.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.col("l.country") != "JP"), "inner")
    .select(
        "s.device",
        "s.listener_id",
        "s.source",
        "s.max_play_ms_played_to_ms_played_bucket_ratio",
        "s.source_list",
        "s.distinct_sources",
        F.col("l.birth_year").alias("listener_birth_year"),
    )
)

result = (
    with_parent_listener
    .select(
        "device",
        "listener_id",
        "max_play_ms_played_to_ms_played_bucket_ratio",
        "source_list",
        "distinct_sources",
        F.concat(F.col("device"), F.lit("#"), F.col("listener_id").cast("string")).alias("device_ref"),
        F.substring(F.trim(F.col("source")), 1, 3).alias("source_prefix"),
    )
    .orderBy(F.col("device").desc(), "max_play_ms_played_to_ms_played_bucket_ratio")
)
