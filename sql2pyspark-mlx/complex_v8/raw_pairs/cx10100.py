from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("artist_id").orderBy(F.col("bpm").desc(), F.col("id").desc())
w_all_by_bpm_per_duration_sec = Window.orderBy("bpm_per_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_bpm_per_duration_sec_2 = Window.orderBy("bpm_per_duration_sec", "id")
w_all_by_bpm_per_duration_sec_3 = Window.orderBy("bpm_per_duration_sec", "id").rowsBetween(-2, Window.currentRow)
w_artist_id = Window.partitionBy("artist_id")

recent = (
    df_tracks
    .filter(F.upper(F.trim(F.col("title"))).like("S%"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        F.least(F.col("bpm"), F.col("duration_sec")).alias("least_bpm_duration_sec"),
        F.round(F.col("bpm") / F.nullif(F.col("duration_sec"), F.lit(0)), 3).alias("bpm_per_duration_sec"),
    )
)

deduped = (
    recent
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_windows = (
    deduped
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        "least_bpm_duration_sec",
        "bpm_per_duration_sec",
        F.max("bpm_per_duration_sec").over(w_all_by_bpm_per_duration_sec).alias("max_bpm_per_duration_sec_so_far"),
        F.row_number().over(w_all_by_bpm_per_duration_sec_2).alias("bpm_per_duration_sec_seq"),
        F.round(F.avg("bpm_per_duration_sec").over(w_all_by_bpm_per_duration_sec_3), 2).alias("bpm_per_duration_sec_avg_3"),
        F.count(F.lit(1)).over(w_artist_id).alias("rows_in_artist_id"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.max("release_date").alias("last_track_release_date"),
        F.max("duration_sec").alias("max_track_duration_sec"),
    )
)

with_counts = (
    with_windows.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select("s.*", "t.last_track_release_date", "t.max_track_duration_sec")
)

flattened = (
    with_counts
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "least_bpm_duration_sec",
        "bpm_per_duration_sec",
        "max_bpm_per_duration_sec_so_far",
        "bpm_per_duration_sec_seq",
        "bpm_per_duration_sec_avg_3",
        "rows_in_artist_id",
        "last_track_release_date",
        "max_track_duration_sec",
        F.explode("moods").alias("mood"),
    )
)

combined_details_artist = (
    flattened.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.bpm",
        "s.least_bpm_duration_sec",
        "s.max_bpm_per_duration_sec_so_far",
        "s.bpm_per_duration_sec_seq",
        "s.bpm_per_duration_sec_avg_3",
        "s.rows_in_artist_id",
        "s.last_track_release_date",
        "s.max_track_duration_sec",
        "s.mood",
        "a.artist_name",
        F.col("a.genre").alias("artist_genre"),
    )
)

result = (
    combined_details_artist
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "bpm",
        "max_bpm_per_duration_sec_so_far",
        "bpm_per_duration_sec_seq",
        "bpm_per_duration_sec_avg_3",
        "rows_in_artist_id",
        "last_track_release_date",
        "artist_name",
        "artist_genre",
        F.greatest(F.col("bpm"), F.col("bpm_per_duration_sec_seq")).alias("greatest_bpm_bpm_per_duration_sec_seq"),
        F.dayofweek(F.col("last_track_release_date")).alias("last_track_release_date_dayofweek"),
    )
    .orderBy(F.col("bpm_per_duration_sec_seq").desc(), F.col("artist_id").desc())
)
