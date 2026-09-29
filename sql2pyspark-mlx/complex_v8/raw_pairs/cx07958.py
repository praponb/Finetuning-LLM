from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id = Window.partitionBy("artist_id")
w_is_explicit_artist_id_by_bpm = Window.partitionBy("is_explicit", "artist_id").orderBy("bpm", "id")
w_is_explicit_by_release_date = Window.partitionBy("is_explicit").orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base_tracks = df_tracks.filter(F.col("artist_id").isNotNull())

ranked = (
    base_tracks
    .select(
        "*",
        F.min("duration_sec").over(w_artist_id).alias("artist_id_min_duration_sec"),
        F.round(F.avg("duration_sec").over(w_artist_id), 2).alias("artist_id_avg_duration_sec"),
        F.ntile(4).over(w_is_explicit_artist_id_by_bpm).alias("bpm_quartile"),
        F.last("release_date").over(w_is_explicit_by_release_date).alias("last_release_date"),
    )
)

result = (
    ranked
    .groupingSets(
        [["artist_id", "bpm_quartile"], ["artist_id"], []],
        "artist_id", "bpm_quartile",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.round(F.sum("artist_id_avg_duration_sec") / F.nullif(F.sum("bpm"), F.lit(0)), 3).alias("artist_id_avg_duration_sec_to_bpm_ratio"),
    )
)
