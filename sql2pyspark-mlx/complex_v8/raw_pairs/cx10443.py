from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_genre_is_skipped = Window.partitionBy("artist_genre", "is_skipped")
w_artist_genre_is_skipped_by_m = Window.partitionBy("artist_genre", "is_skipped").orderBy(F.col("ms_played").desc_nulls_first(), "id")
w_is_skipped_track_id = Window.partitionBy("is_skipped", "track_id")
w_artist_genre_is_skipped_by_m_2 = Window.partitionBy("artist_genre", "is_skipped").orderBy(F.col("ms_played").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_ms_played_full = Window.orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_track_id_is_skipped_by_ms_pl = Window.partitionBy("track_id", "is_skipped").orderBy("ms_played", "id")
w_track_id_listener_id_by_ms_p = Window.partitionBy("track_id", "listener_id").orderBy("ms_played", "id")
w_track_id_is_skipped_by_ms_pl_2 = Window.partitionBy("track_id", "is_skipped").orderBy("ms_played", "id").rowsBetween(-2, -1)

valid = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
    )
)

enriched = (
    valid.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("t.artist_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.is_skipped",
        F.col("t.moods").alias("track_moods"),
        "a.artist_name",
        F.col("a.genre").alias("artist_genre"),
    )
)

ranked = (
    enriched
    .select(
        "*",
        F.count(F.lit(1)).over(w_artist_genre_is_skipped).alias("rows_in_artist_genre_is_skipped"),
        F.rank().over(w_artist_genre_is_skipped_by_m).alias("ms_played_rank"),
        F.size(F.collect_set("listener_id").over(w_is_skipped_track_id)).alias("distinct_listener_id_in_is_skipped_track_id"),
        F.last("ms_played").over(w_artist_genre_is_skipped_by_m_2).alias("last_ms_played"),
        F.nth_value("ms_played", 2).over(w_all_by_ms_played_full).alias("second_ms_played"),
        F.round(F.cume_dist().over(w_track_id_is_skipped_by_ms_pl), 3).alias("ms_played_cume_dist"),
        F.lag("ms_played", 3).over(w_track_id_listener_id_by_ms_p).alias("prev3_ms_played"),
        (F.col("ms_played") - F.lag("ms_played").over(w_track_id_is_skipped_by_ms_pl)).alias("ms_played_change"),
        F.size(F.collect_set("artist_genre").over(w_is_skipped_track_id)).alias("distinct_artist_genre_in_is_skipped_track_id"),
        F.round(F.avg("ms_played").over(w_track_id_is_skipped_by_ms_pl_2), 2).alias("ms_played_avg_2_prior"),
        F.max("ms_played").over(w_artist_genre_is_skipped).alias("artist_genre_is_skipped_max_ms_played"),
    )
)

top_rows = (
    ranked
    .filter((F.col("ms_played_rank") == 1) & (F.col("distinct_listener_id_in_is_skipped_track_id") < 8))
)

rollup_base = (
    top_rows
    .groupBy("listener_id")
    .agg(
        F.min("ms_played_avg_2_prior").alias("min_ms_played_avg_2_prior"),
        F.round(F.sum("second_ms_played") / F.nullif(F.sum("ms_played"), F.lit(0)), 3).alias("second_ms_played_to_ms_played_ratio"),
        F.percentile_approx("ms_played", 0.5).alias("median_ms_played"),
        F.sum("distinct_artist_genre_in_is_skipped_track_id").alias("sum_distinct_artist_genre_in_is_skipped_track_id"),
    )
    .filter(F.col("sum_distinct_artist_genre_in_is_skipped_track_id") > 3)
    .drop("sum_distinct_artist_genre_in_is_skipped_track_id")
)

result = (
    rollup_base
    .select(
        "listener_id",
        "median_ms_played",
        F.coalesce(F.col("median_ms_played"), F.lit(0)).alias("median_ms_played_filled"),
    )
    .orderBy(F.col("listener_id").desc())
)
