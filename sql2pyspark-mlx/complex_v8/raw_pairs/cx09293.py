from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_by_least_duration = Window.partitionBy("artist_id").orderBy("least_duration_sec_bpm")
w_play_is_skipped_count = Window.partitionBy("play_is_skipped_count")
w_artist_id_by_least_duration_2 = Window.partitionBy("artist_id").orderBy("least_duration_sec_bpm", "id")
w_latest = Window.partitionBy("artist_genre").orderBy(F.col("bpm").desc(), F.col("id").desc())
w_release_date_month_artist_ge = Window.partitionBy(F.date_trunc("month", "release_date"), "artist_genre").orderBy("max_play_ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id = Window.partitionBy("artist_id")
w_release_date_month_artist_ge_2 = Window.partitionBy(F.date_trunc("month", "release_date"), "artist_genre").orderBy("max_play_ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent = (
    df_tracks
    .filter(F.size(F.col("moods")) >= 2)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        F.substring(F.trim(F.col("title")), 1, 2).alias("title_prefix"),
        F.least(F.col("duration_sec"), F.col("bpm")).alias("least_duration_sec_bpm"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.max("played_at").alias("last_played_at"),
        F.max("ms_played").alias("max_play_ms_played"),
        F.count(F.when(~F.col("is_skipped"), 1)).alias("play_is_skipped_count"),
    )
)

with_stats = (
    recent.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "p.last_played_at",
        "p.max_play_ms_played",
        F.coalesce(F.col("p.play_is_skipped_count"), F.lit(0)).alias("play_is_skipped_count"),
    )
)

joined_artist = (
    with_stats.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select("s.*", F.col("a.genre").alias("artist_genre"))
)

with_trends = (
    joined_artist
    .select(
        "*",
        F.round(F.cume_dist().over(w_artist_id_by_least_duration), 3).alias("least_duration_sec_bpm_cume_dist"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_play_is_skipped_count), 2).alias("duration_sec_vs_play_is_skipped_count_avg"),
        F.lag("least_duration_sec_bpm").over(w_artist_id_by_least_duration_2).alias("prev_least_duration_sec_bpm"),
        F.round(F.col("least_duration_sec_bpm") / F.nullif(F.sum("least_duration_sec_bpm").over(w_play_is_skipped_count), F.lit(0)), 4).alias("least_duration_sec_bpm_share_of_play_is_skipped_"),
        F.ntile(4).over(w_artist_id_by_least_duration_2).alias("least_duration_sec_bpm_quartile"),
    )
)

filtered = (
    with_trends.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.source").isin("search", "album", "radio")), "left_anti")
)

deduped = (
    filtered
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

metrics = (
    deduped
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "bpm",
        "title_prefix",
        "least_duration_sec_bpm",
        "max_play_ms_played",
        "play_is_skipped_count",
        "artist_genre",
        "duration_sec_vs_play_is_skipped_count_avg",
        "prev_least_duration_sec_bpm",
        "least_duration_sec_bpm_share_of_play_is_skipped_",
        "least_duration_sec_bpm_quartile",
        F.count(F.lit(1)).over(w_release_date_month_artist_ge).alias("running_count"),
        F.sum("least_duration_sec_bpm").over(w_artist_id).alias("artist_id_sum_least_duration_sec_bpm"),
        F.nth_value("least_duration_sec_bpm", 2).over(w_release_date_month_artist_ge_2).alias("second_least_duration_sec_bpm"),
    )
)

result = (
    metrics
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "bpm",
        "title_prefix",
        "least_duration_sec_bpm",
        "max_play_ms_played",
        "play_is_skipped_count",
        "artist_genre",
        "duration_sec_vs_play_is_skipped_count_avg",
        "least_duration_sec_bpm_share_of_play_is_skipped_",
        "least_duration_sec_bpm_quartile",
        "running_count",
        "second_least_duration_sec_bpm",
        F.concat(F.col("artist_genre"), F.lit("#"), F.col("id").cast("string")).alias("artist_genre_ref"),
    )
    .orderBy(
        F.col("artist_genre").asc_nulls_last(),
        F.col("title_prefix").asc_nulls_last(),
        "id",
    )
    .limit(20)
)
