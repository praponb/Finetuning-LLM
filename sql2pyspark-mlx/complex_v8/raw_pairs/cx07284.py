from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit = Window.partitionBy("is_explicit")
w_artist_is_verified_by_durati = Window.partitionBy("artist_is_verified").orderBy(F.col("duration_sec").desc_nulls_first(), "id").rowsBetween(-2, Window.currentRow)
w_artist_is_verified_is_explic = Window.partitionBy("artist_is_verified", "is_explicit").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_is_verified_by_durati_2 = Window.partitionBy("artist_is_verified").orderBy(F.col("duration_sec").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit_artist_id_by_dur = Window.partitionBy("is_explicit", "artist_id").orderBy("duration_sec")
w_artist_is_verified_by_durati_3 = Window.partitionBy("artist_is_verified").orderBy(F.col("duration_sec").desc_nulls_first(), "id")
w_artist_id_is_explicit = Window.partitionBy("artist_id", "is_explicit")

scoped_tracks = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        F.substring(F.trim(F.col("title")), 1, 2).alias("title_prefix"),
    )
)

combined_details_artist = (
    scoped_tracks.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.country") == "US"), "inner")
    .filter(F.col("a.artist_name").isNotNull())
    .select(
        "s.*",
        F.col("a.is_verified").alias("artist_is_verified"),
        "a.artist_name",
    )
)

with_windows = (
    combined_details_artist
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "title_prefix",
        "artist_is_verified",
        F.sum("duration_sec").over(w_is_explicit).alias("is_explicit_sum_duration_sec"),
        F.round(F.avg("bpm").over(w_artist_is_verified_by_durati), 2).alias("bpm_avg_3"),
        F.size(F.collect_set("artist_id").over(w_is_explicit)).alias("distinct_artist_id_in_is_explicit"),
        F.nth_value("duration_sec", 2).over(w_artist_is_verified_is_explic).alias("second_duration_sec"),
        F.first("duration_sec", ignorenulls=True).over(w_artist_is_verified_by_durati_2).alias("first_known_duration_sec"),
        F.round(F.cume_dist().over(w_is_explicit_artist_id_by_dur), 3).alias("duration_sec_cume_dist"),
        F.lag("bpm").over(w_artist_is_verified_by_durati_3).alias("prev_bpm"),
        F.min("duration_sec").over(w_artist_id_is_explicit).alias("artist_id_is_explicit_min_duration_sec"),
        (F.col("bpm") - F.lag("bpm").over(w_artist_is_verified_by_durati_3)).alias("bpm_change"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "artist_id",
        "is_explicit",
        "title_prefix",
        "bpm_avg_3",
        "distinct_artist_id_in_is_explicit",
        "second_duration_sec",
        "first_known_duration_sec",
        "duration_sec_cume_dist",
        "prev_bpm",
        "artist_id_is_explicit_min_duration_sec",
        "bpm_change",
    )
    .orderBy(F.col("first_known_duration_sec").desc())
)
