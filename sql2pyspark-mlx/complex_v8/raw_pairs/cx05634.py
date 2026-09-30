from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("track_id", "listener_id").orderBy(F.col("first_played_at").desc(), F.col("id").desc())
w_device_play_source_count_by = Window.partitionBy("device", "play_source_count").orderBy("last_played_at", "id")
w_source = Window.partitionBy("source")
w_device_play_source_count_by_2 = Window.partitionBy("device", "play_source_count").orderBy("last_played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_plays = (
    df_plays
    .filter(F.col("source").isin("playlist", "album", "radio"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        F.concat_ws(" / ", F.col("source"), F.col("device")).alias("source_device_label"),
        F.when(F.col("source").isin("playlist", "radio"), "group_a").when(F.col("source") == "album", "group_b").otherwise("other").alias("source_group"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.count(F.when(~F.col("source").isin("playlist", "album"), 1)).alias("play_source_count"),
        F.max("played_at").alias("last_played_at"),
        F.min("played_at").alias("first_played_at"),
    )
)

profiled = (
    recent_plays.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_source_count"), F.lit(0)).alias("play_source_count"),
        "p.last_played_at",
        "p.first_played_at",
    )
)

latest_per_key = (
    profiled
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

with_trends = (
    latest_per_key
    .select(
        "*",
        F.ntile(4).over(w_device_play_source_count_by).alias("last_played_at_quartile"),
        F.count(F.lit(1)).over(w_source).alias("rows_in_source"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_source), 2).alias("ms_played_vs_source_avg"),
        F.sum("ms_played").over(w_device_play_source_count_by_2).alias("running_ms_played"),
    )
)

with_details_listener = (
    with_trends.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.source_device_label",
        "s.source_group",
        "s.play_source_count",
        "s.first_played_at",
        "s.last_played_at_quartile",
        "s.rows_in_source",
        "s.ms_played_vs_source_avg",
        "s.running_ms_played",
        F.col("l.birth_year").alias("listener_birth_year"),
    )
)

combined_details_listener = (
    with_details_listener.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.source_device_label",
        "s.source_group",
        "s.play_source_count",
        "s.rows_in_source",
        "s.ms_played_vs_source_avg",
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("l.birth_year").alias("listener_birth_year_2"),
        F.col("l.plan").alias("listener_plan"),
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("t.title").alias("track_title"),
    )
)

result = (
    combined_details_listener
    .filter(F.col("listener_signup_date") >= F.lit("2024-08-01").cast("date"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "source_device_label",
        "source_group",
        "play_source_count",
        "rows_in_source",
        "listener_plan",
        "track_is_explicit",
        "track_title",
        F.when(F.col("ms_played_vs_source_avg") >= 80000.0, "large").when(F.col("ms_played_vs_source_avg") >= -240000.0, "medium").otherwise("small").alias("ms_played_vs_source_avg_tier"),
        F.initcap(F.trim(F.col("source_group"))).alias("source_group_clean"),
    )
    .orderBy(F.col("track_title").desc_nulls_first(), F.col("listener_id").asc())
)
