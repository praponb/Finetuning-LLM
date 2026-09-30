from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_rows_is_explicit = Window.partitionBy("n_rows", "is_explicit")
w_is_explicit_distinct_artist = Window.partitionBy("is_explicit", "distinct_artist_ids").orderBy("distinct_ids", "is_explicit").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_artist_ids_is_expli = Window.partitionBy("distinct_artist_ids", "is_explicit").orderBy("max_duration_sec", "is_explicit")

scoped_tracks = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.max("release_date").alias("last_track_release_date"),
        F.sum("duration_sec").alias("total_track_duration_sec"),
    )
)

with_counts = (
    scoped_tracks.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select("s.*", "t.last_track_release_date", "t.total_track_duration_sec")
)

rollup_base = (
    with_counts
    .groupBy("is_explicit")
    .agg(
        F.count("*").alias("n_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("artist_id").alias("distinct_artist_ids"),
        F.sum(F.when(F.upper(F.trim(F.col("title"))).like("G%"), F.col("bpm")).otherwise(0)).alias("bpm_upper_trim_title_like_g"),
        F.min("bpm").alias("min_bpm"),
        F.max("duration_sec").alias("max_duration_sec"),
    )
)

sequenced = (
    rollup_base
    .select(
        "*",
        F.round(F.col("bpm_upper_trim_title_like_g") - F.avg("bpm_upper_trim_title_like_g").over(w_n_rows_is_explicit), 2).alias("bpm_upper_trim_title_like_g_vs_n_rows_is_explici"),
        F.sum("min_bpm").over(w_is_explicit_distinct_artist).alias("running_min_bpm"),
        F.dense_rank().over(w_distinct_artist_ids_is_expli).alias("max_duration_sec_dense_rank"),
    )
)

result = (
    sequenced
    .groupingSets(
        [["distinct_artist_ids", "n_rows"], ["distinct_artist_ids"], []],
        "distinct_artist_ids", "n_rows",
    )
    .agg(
        F.sum("distinct_ids").alias("total_distinct_ids"),
        F.max("distinct_artist_ids").alias("max_distinct_artist_ids"),
        F.count(F.when(F.col("is_explicit"), 1)).alias("n_is_explicit_true"),
    )
    .orderBy(
        F.col("distinct_artist_ids").asc_nulls_last(),
        F.col("n_rows").asc_nulls_last(),
    )
)
