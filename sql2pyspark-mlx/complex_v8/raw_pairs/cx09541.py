from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_bpm_per_duration_sec = Window.orderBy("bpm_per_duration_sec", "id", "mood").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_bpm_per_duration_sec_2 = Window.orderBy("bpm_per_duration_sec", "id", "mood").rowsBetween(-6, Window.currentRow)
w_artist_is_verified_artist_id = Window.partitionBy("artist_is_verified", "artist_id")
w_artist_id_artist_is_verified = Window.partitionBy("artist_id", "artist_is_verified")
w_mood_artist_id_by_first_dura = Window.partitionBy("mood", "artist_id").orderBy(F.col("first_duration_sec").desc(), "id", "mood")

filtered_tracks = (
    df_tracks
    .filter(F.col("artist_id") <= 2)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.round(F.col("bpm") / F.nullif(F.col("duration_sec"), F.lit(0)), 3).alias("bpm_per_duration_sec"),
    )
)

enriched_artist = (
    filtered_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.bpm",
        "s.moods",
        "s.bpm_per_duration_sec",
        "a.artist_name",
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

flattened = (
    enriched_artist
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "bpm_per_duration_sec",
        "artist_name",
        "artist_debut_year",
        "artist_is_verified",
        F.explode("moods").alias("mood"),
    )
)

ranked = (
    flattened
    .select(
        "*",
        F.first("duration_sec").over(w_all_by_bpm_per_duration_sec).alias("first_duration_sec"),
        F.round(F.avg("bpm_per_duration_sec").over(w_all_by_bpm_per_duration_sec_2), 2).alias("bpm_per_duration_sec_avg_7"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_artist_is_verified_artist_id), 2).alias("duration_sec_vs_artist_is_verified_artist_id_avg"),
    )
)

with_activity = (
    ranked.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.month(F.col("p.played_at")).isin(2, 3)), "left_semi")
)

with_trends = (
    with_activity
    .select(
        "id",
        "title",
        "duration_sec",
        "bpm_per_duration_sec",
        "artist_name",
        "artist_debut_year",
        "artist_is_verified",
        "mood",
        "first_duration_sec",
        "bpm_per_duration_sec_avg_7",
        "duration_sec_vs_artist_is_verified_artist_id_avg",
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_artist_id_artist_is_verified), 2).alias("duration_sec_vs_artist_id_artist_is_verified_avg"),
        F.lag("bpm_per_duration_sec_avg_7", 1, 0).over(w_mood_artist_id_by_first_dura).alias("prev_bpm_per_duration_sec_avg_7"),
        F.lead("first_duration_sec", 1, 0).over(w_mood_artist_id_by_first_dura).alias("next_first_duration_sec"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "mood",
        "first_duration_sec",
        "next_first_duration_sec",
        "artist_debut_year",
    )
    .unpivot(
        ids=["id", "mood"],
        values=["first_duration_sec", "next_first_duration_sec", "artist_debut_year"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "mood", "metric_name")
)
