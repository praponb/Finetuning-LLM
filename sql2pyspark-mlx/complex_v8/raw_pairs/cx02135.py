from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("listener_id").orderBy(F.col("ms_played_distance").desc(), F.col("id").desc())

base_plays = (
    df_plays
    .filter((F.col("track_id").isNotNull()) & (F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        F.lower(F.regexp_replace(F.trim(F.col("device")), " ", "_")).alias("device_slug"),
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
        F.abs(F.col("ms_played") - 183484).alias("ms_played_distance"),
    )
)

labeled = (
    base_plays
    .select(
        "id",
        "listener_id",
        "played_at",
        "device",
        "device_slug",
        "is_skipped_label",
        "ms_played_distance",
        F.concat(F.col("is_skipped_label"), F.lit("#"), F.col("track_id").cast("string")).alias("is_skipped_label_ref"),
        F.length(F.trim(F.col("is_skipped_label"))).alias("is_skipped_label_len"),
    )
)

unique_rows = (
    labeled
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

exploded_detail_plays = (
    unique_rows.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.track_id").isNotNull()), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.device",
        "s.is_skipped_label_ref",
        "s.is_skipped_label_len",
        F.col("p.id").alias("play_id"),
        F.col("p.source").alias("play_source"),
        F.col("p.track_id").alias("play_track_id"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.min("played_at").alias("first_played_at"),
        F.sum("ms_played").alias("total_play_ms_played"),
        F.round(F.avg("ms_played"), 2).alias("avg_play_ms_played"),
    )
)

with_stats = (
    exploded_detail_plays.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.play_track_id"), "left")
    .select(
        "s.*",
        "p.first_played_at",
        "p.total_play_ms_played",
        "p.avg_play_ms_played",
    )
)

with_details = (
    with_stats.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .filter(F.col("l.country") == "KR")
    .select(
        "s.id",
        "s.listener_id",
        "s.play_id",
        "s.play_source",
        "s.play_track_id",
        "s.avg_play_ms_played",
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("l.plan").alias("listener_plan"),
        F.col("l.country").alias("listener_country"),
    )
)

play_source_stats = (
    with_details
    .groupBy("play_source")
    .agg(F.avg("avg_play_ms_played").alias("avg_avg_play_ms_played"))
)

vs_group = (
    with_details.alias("s")
    .join(play_source_stats.alias("g"), F.col("g.play_source") == F.col("s.play_source"), "inner")
    .filter(F.col("s.avg_play_ms_played") > F.col("g.avg_avg_play_ms_played"))
    .select(
        "s.*",
        "g.avg_avg_play_ms_played",
        F.round(F.col("s.avg_play_ms_played") - F.col("g.avg_avg_play_ms_played"), 2).alias("avg_play_ms_played_minus_avg"),
    )
)

result = (
    vs_group
    .select(
        "id",
        "listener_id",
        "play_id",
        "play_source",
        "avg_play_ms_played",
        "listener_signup_date",
        "listener_plan",
        "avg_play_ms_played_minus_avg",
        (F.col("avg_avg_play_ms_played") + F.col("avg_play_ms_played")).alias("avg_avg_play_ms_played_plus_avg_play_ms_played"),
    )
    .orderBy("play_id", F.col("id").desc_nulls_first())
)
