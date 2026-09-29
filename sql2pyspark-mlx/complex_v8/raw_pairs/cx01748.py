from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_verified_genre = Window.partitionBy("is_verified", "genre")
w_is_verified_by_min_track_dur = Window.partitionBy("is_verified").orderBy(F.col("min_track_duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_artists = (
    df_artists
    .filter(~F.col("is_verified"))
    .select(
        "id",
        "genre",
        "country",
        "is_verified",
        F.when(F.col("genre").isin("Pop", "Jazz"), "group_a").when(F.col("genre") == "HipHop", "group_b").otherwise("other").alias("genre_group"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(F.min("duration_sec").alias("min_track_duration_sec"))
)

with_counts = (
    prepared_artists.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left")
    .select("s.*", "t.min_track_duration_sec")
)

metrics = (
    with_counts
    .select(
        "*",
        F.count(F.lit(1)).over(w_is_verified_genre).alias("rows_in_is_verified_genre"),
        F.nth_value("min_track_duration_sec", 2).over(w_is_verified_by_min_track_dur).alias("second_min_track_duration_sec"),
        F.sum("min_track_duration_sec").over(w_is_verified_genre).alias("is_verified_genre_sum_min_track_duration_sec"),
    )
)

result = (
    metrics
    .groupBy("country", "is_verified")
    .pivot("genre_group", ["other", "group_a"])
    .agg(F.sum("rows_in_is_verified_genre"))
)
