from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_play_track_id_play_id = Window.partitionBy("play_track_id", "play_id")
w_play_id_track_is_explicit_by = Window.partitionBy("play_id", "track_is_explicit").orderBy("birth_year", "id", "play_id")
w_play_id_track_is_explicit_by_2 = Window.partitionBy("play_id", "track_is_explicit").orderBy("birth_year")
w_all_by_birth_year_running = Window.orderBy(F.col("birth_year").desc(), "id", "play_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_is_explicit = Window.partitionBy("track_is_explicit")
w_play_source_by_prev_birth_ye = Window.partitionBy("play_source").orderBy("prev_birth_year", "id", "play_id")
w_birth_year_rank = Window.partitionBy("birth_year_rank")
w_play_track_id_play_device_by = Window.partitionBy("play_track_id", "play_device").orderBy(F.col("prev_birth_year").desc(), "id", "play_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_play_track_id_play_device_by_2 = Window.partitionBy("play_track_id", "play_device").orderBy(F.col("prev_birth_year").desc(), "id", "play_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid = (
    df_listeners
    .filter((F.col("signup_date").isNotNull()) & (F.length(F.trim(F.col("username"))) >= 4))
    .select(
        "id",
        "username",
        "plan",
        "birth_year",
    )
)

exploded_detail = (
    valid.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.birth_year",
        F.col("p.id").alias("play_id"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.device").alias("play_device"),
        F.col("p.track_id").alias("play_track_id"),
        F.col("p.source").alias("play_source"),
    )
)

joined_track = (
    exploded_detail.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.play_track_id"), "inner")
    .join(df_plays.alias("p"), F.col("p.id") == F.col("s.play_id"), "inner")
    .select(
        "s.id",
        "s.birth_year",
        "s.play_id",
        "s.play_device",
        "s.play_track_id",
        "s.play_source",
        F.col("t.moods").alias("track_moods"),
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("t.title").alias("track_title"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

windowed = (
    joined_track
    .select(
        "id",
        "birth_year",
        "play_id",
        "play_device",
        "play_track_id",
        "play_source",
        "track_is_explicit",
        "track_title",
        "play_ms_played",
        F.round(F.col("play_ms_played") - F.avg("play_ms_played").over(w_play_track_id_play_id), 2).alias("play_ms_played_vs_play_track_id_play_id_avg"),
        F.lag("birth_year").over(w_play_id_track_is_explicit_by).alias("prev_birth_year"),
        F.round(F.percent_rank().over(w_play_id_track_is_explicit_by_2), 3).alias("birth_year_percent_rank"),
        F.round(F.avg("play_ms_played").over(w_play_track_id_play_id), 2).alias("play_track_id_play_id_avg_play_ms_played"),
        F.rank().over(w_play_id_track_is_explicit_by).alias("birth_year_rank"),
    )
)

play_source_stats = (
    windowed
    .groupBy("play_source")
    .agg(F.max("prev_birth_year").alias("max_prev_birth_year"))
)

compared = (
    windowed.alias("s")
    .join(play_source_stats.alias("g"), F.col("g.play_source") == F.col("s.play_source"), "inner")
    .filter(F.col("s.prev_birth_year") == F.col("g.max_prev_birth_year"))
    .select(
        "s.*",
        "g.max_prev_birth_year",
        F.round(F.col("s.prev_birth_year") - F.col("g.max_prev_birth_year"), 2).alias("prev_birth_year_minus_max"),
    )
)

windowed_2 = (
    compared
    .select(
        "*",
        F.last("prev_birth_year_minus_max", ignorenulls=True).over(w_all_by_birth_year_running).alias("last_known_prev_birth_year_minus_max"),
        F.max("birth_year_rank").over(w_track_is_explicit).alias("track_is_explicit_max_birth_year_rank"),
        F.round(F.col("prev_birth_year") / F.nullif(F.sum("prev_birth_year").over(w_track_is_explicit), F.lit(0)), 4).alias("prev_birth_year_share_of_track_is_explicit"),
        F.count(F.lit(1)).over(w_track_is_explicit).alias("rows_in_track_is_explicit"),
        F.row_number().over(w_play_source_by_prev_birth_ye).alias("rn"),
    )
)

sequenced = (
    windowed_2
    .select(
        "*",
        F.max("prev_birth_year").over(w_birth_year_rank).alias("birth_year_rank_max_prev_birth_year"),
        F.last("prev_birth_year_minus_max", ignorenulls=True).over(w_play_track_id_play_device_by).alias("last_known_prev_birth_year_minus_max_2"),
        F.size(F.collect_set("play_device").over(w_birth_year_rank)).alias("distinct_play_device_in_birth_year_rank"),
        F.last("max_prev_birth_year").over(w_play_track_id_play_device_by_2).alias("last_max_prev_birth_year"),
        F.first("rows_in_track_is_explicit").over(w_play_track_id_play_device_by_2).alias("first_rows_in_track_is_explicit"),
    )
)

result = (
    sequenced
    .filter(F.col("play_ms_played_vs_play_track_id_play_id_avg") <= 240000.0)
    .select(
        "id",
        "birth_year",
        "play_id",
        "play_device",
        "play_track_id",
        "play_source",
        "track_is_explicit",
        "track_title",
        "play_ms_played",
        "prev_birth_year",
        "birth_year_percent_rank",
        "max_prev_birth_year",
        "prev_birth_year_minus_max",
        "track_is_explicit_max_birth_year_rank",
        "prev_birth_year_share_of_track_is_explicit",
        "rn",
        "birth_year_rank_max_prev_birth_year",
        "last_known_prev_birth_year_minus_max_2",
        "last_max_prev_birth_year",
        (F.col("birth_year_percent_rank") + F.col("rn")).alias("birth_year_percent_rank_plus_rn"),
        F.when(F.col("play_source").isin("radio", "album"), "group_a").when(F.col("play_source") == "playlist", "group_b").otherwise("other").alias("play_source_group"),
    )
    .orderBy("play_device")
)
