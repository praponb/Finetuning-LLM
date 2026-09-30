from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_bpm_tier_artist_id_by_durati = Window.partitionBy("bpm_tier", "artist_id").orderBy(F.col("duration_sec").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id = Window.partitionBy("artist_id")
w_bpm_tier_artist_id_by_durati_2 = Window.partitionBy("bpm_tier", "artist_id").orderBy(F.col("duration_sec").asc_nulls_last(), "id")
w_all_by_bpm_distance_moving_1 = Window.orderBy("bpm_distance", "id").rowsBetween(-1, Window.currentRow)
w_all_by_bpm_distance = Window.orderBy("bpm_distance", "id")

cleaned_tracks = (
    df_tracks
    .filter((F.lower(F.col("title")).like("%ty%")) & (F.col("duration_sec").between(142, 352)))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        F.date_format(F.col("release_date"), "yyyy-MM").alias("release_date_label"),
        F.abs(F.col("bpm") - 96).alias("bpm_distance"),
        F.when(F.col("bpm") >= 163, "HIGH").when(F.col("bpm") >= 89, "MEDIUM").otherwise("LOW").alias("bpm_tier"),
    )
)

metrics = (
    cleaned_tracks
    .select(
        "*",
        F.last("bpm_distance").over(w_bpm_tier_artist_id_by_durati).alias("last_bpm_distance"),
        F.max("bpm").over(w_artist_id).alias("artist_id_max_bpm"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_artist_id), 2).alias("duration_sec_vs_artist_id_avg"),
        F.rank().over(w_bpm_tier_artist_id_by_durati_2).alias("duration_sec_rank"),
        F.ntile(10).over(w_bpm_tier_artist_id_by_durati_2).alias("duration_sec_decile"),
        F.round(F.avg("duration_sec").over(w_artist_id), 2).alias("artist_id_avg_duration_sec"),
    )
)

sequenced = (
    metrics
    .select(
        "*",
        F.round(F.avg("last_bpm_distance").over(w_all_by_bpm_distance_moving_1), 2).alias("last_bpm_distance_avg_2"),
        (F.col("duration_sec_decile") - F.lag("duration_sec_decile").over(w_all_by_bpm_distance)).alias("duration_sec_decile_change"),
    )
)

labeled = (
    sequenced
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm_distance",
        "duration_sec_vs_artist_id_avg",
        "duration_sec_rank",
        "duration_sec_decile",
        "artist_id_avg_duration_sec",
        "last_bpm_distance_avg_2",
        "duration_sec_decile_change",
        F.concat_ws(" / ", F.col("release_date_label"), F.col("bpm_tier")).alias("release_date_label_bpm_tier_label"),
    )
)

combined_details = (
    labeled.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.*",
        F.col("a.genre").alias("artist_genre"),
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.country").alias("artist_country"),
    )
)

top_rows = (
    combined_details
    .filter((F.col("duration_sec_rank") <= 5) & (F.col("duration_sec_vs_artist_id_avg") >= -84.0))
)

result = (
    top_rows
    .filter(F.col("last_bpm_distance_avg_2") > 27.0)
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm_distance",
        "duration_sec_vs_artist_id_avg",
        "duration_sec_rank",
        "duration_sec_decile",
        "last_bpm_distance_avg_2",
        "duration_sec_decile_change",
        "release_date_label_bpm_tier_label",
        "artist_genre",
        "artist_debut_year",
        "artist_country",
        F.concat_ws(": ", F.col("artist_genre"), F.col("artist_country")).alias("artist_genre_artist_country_label"),
    )
    .orderBy(F.col("duration_sec_decile_change").desc())
)
