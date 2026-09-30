from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_count_artist_id = Window.partitionBy("track_count", "artist_id")
w_all_by_track_count_ties = Window.orderBy("track_count")
w_all_by_track_count = Window.orderBy("track_count", "id")

cleaned_tracks = (
    df_tracks
    .filter(F.col("duration_sec") > 183)
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.count("*").alias("track_count"),
        F.count("*").alias("track_count_2"),
        F.max("release_date").alias("last_track_release_date"),
    )
)

with_counts = (
    cleaned_tracks.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.track_count"), F.lit(0)).alias("track_count"),
        F.coalesce(F.col("t.track_count_2"), F.lit(0)).alias("track_count_2"),
        "t.last_track_release_date",
    )
)

metrics = (
    with_counts
    .select(
        "*",
        F.round(F.col("track_count_2") - F.avg("track_count_2").over(w_track_count_artist_id), 2).alias("track_count_2_vs_track_count_artist_id_avg"),
        F.rank().over(w_all_by_track_count_ties).alias("track_count_rank"),
        F.round(F.cume_dist().over(w_all_by_track_count_ties), 3).alias("track_count_cume_dist"),
        F.ntile(4).over(w_all_by_track_count).alias("track_count_quartile"),
    )
)

result = (
    metrics
    .groupingSets(
        [["is_explicit", "track_count"], ["is_explicit"]],
        "is_explicit", "track_count",
    )
    .agg(
        F.countDistinct("artist_id").alias("distinct_artist_ids"),
        F.sum("duration_sec").alias("total_duration_sec"),
    )
    .orderBy(
        F.col("is_explicit").asc_nulls_last(),
        F.col("track_count").asc_nulls_last(),
    )
)
