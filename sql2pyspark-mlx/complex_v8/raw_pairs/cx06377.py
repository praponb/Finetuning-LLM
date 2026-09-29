from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_play_id_by_duration_sec_full = Window.partitionBy("play_id").orderBy(F.col("duration_sec").desc(), "id", "play_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_play_played_at_month_play_so = Window.partitionBy(F.date_trunc("month", "play_played_at"), "play_source").orderBy(F.col("play_ms_played").desc(), "id", "play_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_play_id_artist_id = Window.partitionBy("play_id", "artist_id")
w_all_by_release_date_ties = Window.orderBy("release_date")
w_all_by_release_date = Window.orderBy("release_date", "id", "play_id")
w_is_explicit_play_source = Window.partitionBy("is_explicit", "play_source")

filtered = (
    df_tracks
    .filter((F.length(F.trim(F.col("title"))) >= 4) & (F.col("release_date") < F.lit("2025-03-08").cast("date")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
    )
)

line_level = (
    filtered.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        F.col("p.id").alias("play_id"),
        F.col("p.source").alias("play_source"),
        F.col("p.device").alias("play_device"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.played_at").alias("play_played_at"),
    )
)

windowed = (
    line_level
    .select(
        "*",
        F.first("play_ms_played").over(w_play_id_by_duration_sec_full).alias("first_play_ms_played"),
        F.nth_value("duration_sec", 2).over(w_play_played_at_month_play_so).alias("second_duration_sec"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_play_id_artist_id), F.lit(0)), 4).alias("duration_sec_share_of_play_id_artist_id"),
    )
)

with_trends = (
    windowed
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_release_date_ties), 3).alias("release_date_percent_rank"),
        F.lag("play_ms_played", 1, 0).over(w_all_by_release_date).alias("prev_play_ms_played"),
        F.sum("second_duration_sec").over(w_is_explicit_play_source).alias("is_explicit_play_source_sum_second_duration_sec"),
        F.size(F.collect_set("play_id").over(w_is_explicit_play_source)).alias("distinct_play_id_in_is_explicit_play_source"),
        F.count(F.lit(1)).over(w_is_explicit_play_source).alias("rows_in_is_explicit_play_source"),
        F.lag("play_played_at").over(w_all_by_release_date).alias("prev_play_played_at"),
    )
)

filtered_2 = (
    with_trends.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.col("t.bpm") < 142) & (F.lower(F.col("t.title")).like("%eo%")), "left_anti")
    .filter(F.col("is_explicit"))
)

result = (
    filtered_2
    .filter(F.col("play_source") == "album")
    .select(
        "id",
        "artist_id",
        "title",
        "play_source",
        "play_device",
        "play_ms_played",
        "play_played_at",
        "second_duration_sec",
        "release_date_percent_rank",
        "is_explicit_play_source_sum_second_duration_sec",
        "distinct_play_id_in_is_explicit_play_source",
        "rows_in_is_explicit_play_source",
        "prev_play_played_at",
    )
    .orderBy("artist_id", F.col("second_duration_sec").asc())
)
