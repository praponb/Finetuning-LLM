from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_is_verified = Window.partitionBy("artist_is_verified")
w_artist_id = Window.partitionBy("artist_id")
w_all_by_bpm = Window.orderBy("bpm", "id")
w_artist_genre_artist_is_verif = Window.partitionBy("artist_genre", "artist_is_verified").orderBy("bpm", "id")
w_all_by_bpm_running = Window.orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_tracks = (
    df_tracks
    .filter(F.col("is_explicit"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        F.abs(F.col("bpm") - 142).alias("bpm_distance"),
    )
)

combined_details = (
    recent_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(~F.col("a.genre").isin("Pop", "HipHop"))
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.bpm",
        "s.moods",
        F.col("a.genre").alias("artist_genre"),
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

with_windows = (
    combined_details
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "moods",
        "artist_is_verified",
        "artist_debut_year",
        F.round(F.col("artist_debut_year") - F.avg("artist_debut_year").over(w_artist_is_verified), 2).alias("artist_debut_year_vs_artist_is_verified_avg"),
        F.max("artist_debut_year").over(w_artist_id).alias("artist_id_max_artist_debut_year"),
        F.size(F.collect_set("artist_genre").over(w_artist_is_verified)).alias("distinct_artist_genre_in_artist_is_verified"),
        F.round(F.avg("duration_sec").over(w_artist_is_verified), 2).alias("artist_is_verified_avg_duration_sec"),
        F.max("artist_debut_year").over(w_artist_is_verified).alias("artist_is_verified_max_artist_debut_year"),
        F.lead("bpm", 1, 0).over(w_all_by_bpm).alias("next_bpm"),
        F.round(F.col("bpm") - F.avg("bpm").over(w_artist_id), 2).alias("bpm_vs_artist_id_avg"),
        F.lead("duration_sec", 3).over(w_artist_genre_artist_is_verif).alias("next3_duration_sec"),
        F.sum("bpm").over(w_artist_is_verified).alias("artist_is_verified_sum_bpm"),
        F.last("duration_sec", ignorenulls=True).over(w_all_by_bpm_running).alias("last_known_duration_sec"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "artist_id",
        "title",
        "moods",
        "artist_debut_year",
        "distinct_artist_genre_in_artist_is_verified",
        "artist_is_verified_max_artist_debut_year",
        "next_bpm",
        "bpm_vs_artist_id_avg",
        "next3_duration_sec",
        "artist_is_verified_sum_bpm",
        "last_known_duration_sec",
        (F.col("artist_debut_year") - F.col("next3_duration_sec")).alias("artist_debut_year_minus_next3_duration_sec"),
        F.round(F.col("bpm_vs_artist_id_avg") * 0.25, 2).alias("bpm_vs_artist_id_avg_adj"),
    )
    .orderBy(F.col("artist_debut_year").asc(), "artist_is_verified_sum_bpm")
)
