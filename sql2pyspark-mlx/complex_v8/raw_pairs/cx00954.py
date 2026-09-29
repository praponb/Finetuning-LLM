from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_artist_genre_by = Window.partitionBy("is_explicit", "artist_genre").orderBy(F.col("bpm").desc(), "id")
w_artist_id_artist_genre_by_du = Window.partitionBy("artist_id", "artist_genre").orderBy(F.col("duration_sec").desc(), "id").rowsBetween(-3, -1)
w_artist_id_artist_genre_by_du_2 = Window.partitionBy("artist_id", "artist_genre").orderBy(F.col("duration_sec").desc(), "id")
w_all_by_duration_sec = Window.orderBy("duration_sec", "id")
w_artist_id_artist_genre_by_du_3 = Window.partitionBy("artist_id", "artist_genre").orderBy(F.col("duration_sec").desc())
w_artist_genre = Window.partitionBy("artist_genre")
w_all_by_duration_sec_running = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit_artist_genre_by_2 = Window.partitionBy("is_explicit", "artist_genre").orderBy(F.col("bpm").desc(), "id").rowsBetween(-3, Window.currentRow)

valid_tracks = (
    df_tracks
    .filter((F.lower(F.col("title")).like("%mi%")) & (F.col("bpm").between(76, 155)))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
    )
)

combined_details_artist = (
    valid_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.col("a.genre").isNotNull())
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.is_explicit",
        "s.bpm",
        "s.moods",
        "s.title_slug",
        F.col("a.genre").alias("artist_genre"),
    )
)

with_trends = (
    combined_details_artist
    .select(
        "*",
        F.rank().over(w_is_explicit_artist_genre_by).alias("bpm_rank"),
        F.sum("bpm").over(w_artist_id_artist_genre_by_du).alias("bpm_sum_3_prior"),
        (F.col("bpm") - F.lag("bpm").over(w_artist_id_artist_genre_by_du_2)).alias("bpm_change"),
        F.lead("duration_sec", 2).over(w_all_by_duration_sec).alias("next2_duration_sec"),
        F.rank().over(w_artist_id_artist_genre_by_du_3).alias("duration_sec_rank"),
        F.round(F.avg("bpm").over(w_artist_genre), 2).alias("artist_genre_avg_bpm"),
        F.sum("duration_sec").over(w_all_by_duration_sec_running).alias("running_duration_sec"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_artist_genre), F.lit(0)), 4).alias("bpm_share_of_artist_genre"),
        F.round(F.avg("bpm").over(w_is_explicit_artist_genre_by_2), 2).alias("bpm_avg_4"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        "title_slug",
        "artist_genre",
        "bpm_sum_3_prior",
        "duration_sec_rank",
        "artist_genre_avg_bpm",
    )
    .orderBy(F.col("duration_sec_rank").desc())
)
