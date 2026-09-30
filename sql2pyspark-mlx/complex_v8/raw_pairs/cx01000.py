from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_artist_id = Window.partitionBy("is_explicit", "artist_id")
w_artist_id_by_duration_sec_mo = Window.partitionBy("artist_id").orderBy(F.col("duration_sec").desc_nulls_first(), "id").rowsBetween(-2, Window.currentRow)
w_artist_id_is_explicit = Window.partitionBy("artist_id", "is_explicit")
w_artist_id_is_explicit_by_dur = Window.partitionBy("artist_id", "is_explicit").orderBy(F.col("duration_sec_avg_3").desc(), "id")

scoped_tracks = (
    df_tracks
    .filter(F.col("artist_id") >= 12)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "moods",
        F.lower(F.trim(F.col("title"))).alias("title_clean"),
        F.date_format(F.col("release_date"), "yyyy-MM-dd").alias("release_date_label"),
        F.greatest(F.col("duration_sec"), F.col("bpm")).alias("greatest_duration_sec_bpm"),
    )
)

windowed = (
    scoped_tracks
    .select(
        "*",
        F.round(F.avg("greatest_duration_sec_bpm").over(w_is_explicit_artist_id), 2).alias("is_explicit_artist_id_avg_greatest_duration_sec_"),
        F.round(F.avg("duration_sec").over(w_artist_id_by_duration_sec_mo), 2).alias("duration_sec_avg_3"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.max("release_date").alias("last_track_release_date"),
        F.max("bpm").alias("max_track_bpm"),
    )
)

profiled = (
    windowed.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select("s.*", "t.last_track_release_date", "t.max_track_bpm")
)

with_trends = (
    profiled
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "release_date_label",
        "greatest_duration_sec_bpm",
        "duration_sec_avg_3",
        F.max("duration_sec").over(w_artist_id_is_explicit).alias("artist_id_is_explicit_max_duration_sec"),
        F.dense_rank().over(w_artist_id_is_explicit_by_dur).alias("duration_sec_avg_3_dense_rank"),
        F.count(F.lit(1)).over(w_artist_id_is_explicit).alias("rows_in_artist_id_is_explicit"),
        F.datediff(F.col("last_track_release_date"), F.lag("last_track_release_date").over(w_artist_id_is_explicit_by_dur)).alias("days_since_prev_last_track_release_date"),
    )
)

filtered = (
    with_trends.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.col("t.release_date") < F.lit("2025-03-08").cast("date")) & (~F.col("t.is_explicit")), "left_anti")
    .filter(F.col("duration_sec_avg_3") <= 271.5)
)

result = (
    filtered
    .select(
        "id",
        "title",
        "duration_sec",
        "is_explicit",
        "release_date_label",
        "duration_sec_avg_3",
        "artist_id_is_explicit_max_duration_sec",
        "duration_sec_avg_3_dense_rank",
        "rows_in_artist_id_is_explicit",
        "days_since_prev_last_track_release_date",
        F.when(F.col("duration_sec") >= 352, "A").when(F.col("duration_sec") >= 165, "B").otherwise("C").alias("duration_sec_tier"),
    )
    .orderBy(F.col("days_since_prev_last_track_release_date").asc(), "id")
    .limit(20)
)
