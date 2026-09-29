from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_bpm = Window.orderBy("bpm", "id")
w_all_by_bpm_full = Window.orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id = Window.partitionBy("artist_id")
w_artist_id_by_bpm_quintile = Window.partitionBy("artist_id").orderBy("bpm_quintile", "id")
w_artist_id_by_bpm_quintile_fu = Window.partitionBy("artist_id").orderBy("bpm_quintile", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_tracks = (
    df_tracks
    .filter(F.array_contains(F.col("moods"), "chill"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        F.date_format(F.col("release_date"), "yyyy-MM-dd").alias("release_date_label"),
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("release_date")).alias("days_since_release_date"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.min("played_at").alias("first_played_at"),
        F.count(F.when(~F.col("is_skipped"), 1)).alias("play_is_skipped_count"),
        F.max("played_at").alias("last_played_at"),
    )
)

summarized_rows = (
    prepared_tracks.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "p.first_played_at",
        F.coalesce(F.col("p.play_is_skipped_count"), F.lit(0)).alias("play_is_skipped_count"),
        "p.last_played_at",
    )
)

metrics = (
    summarized_rows
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "play_is_skipped_count",
        F.ntile(5).over(w_all_by_bpm).alias("bpm_quintile"),
        F.nth_value("bpm", 2).over(w_all_by_bpm_full).alias("second_bpm"),
    )
)

with_trends = (
    metrics
    .select(
        "*",
        F.lag("bpm", 2).over(w_artist_id_by_bpm_quintile).alias("prev2_bpm"),
        F.count(F.lit(1)).over(w_artist_id).alias("rows_in_artist_id"),
        F.row_number().over(w_artist_id_by_bpm_quintile).alias("seq_no"),
        F.nth_value("second_bpm", 2).over(w_artist_id_by_bpm_quintile_fu).alias("second_second_bpm"),
    )
)

qualified = (
    with_trends.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.upper(F.trim(F.col("a.artist_name"))).like("K%")), "left_anti")
)

result = (
    qualified
    .filter(F.col("duration_sec") > 142)
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "play_is_skipped_count",
        "bpm_quintile",
        "second_bpm",
        "prev2_bpm",
        "rows_in_artist_id",
        "seq_no",
        "second_second_bpm",
        F.length(F.trim(F.col("title"))).alias("title_len"),
    )
    .orderBy(F.col("bpm_quintile").desc(), F.col("second_second_bpm").desc())
)
