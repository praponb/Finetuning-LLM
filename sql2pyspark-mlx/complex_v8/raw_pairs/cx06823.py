from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_by_bpm = Window.partitionBy("artist_id").orderBy(F.col("bpm").desc(), "id", "mood_pos")
w_artist_id_by_bpm_prior_1 = Window.partitionBy("artist_id").orderBy(F.col("bpm").desc(), "id", "mood_pos").rowsBetween(-1, -1)

valid_tracks = (
    df_tracks
    .filter((F.lower(F.col("title")).like("%ea%")) | (F.col("duration_sec") <= 353))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
    )
)

per_label = (
    valid_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        F.posexplode("moods").alias("mood_pos", "mood"),
    )
)

with_windows = (
    per_label
    .select(
        "id",
        "artist_id",
        "bpm",
        "mood_pos",
        "mood",
        (F.col("bpm") - F.lag("bpm").over(w_artist_id_by_bpm)).alias("bpm_change"),
        F.round(F.avg("duration_sec").over(w_artist_id_by_bpm_prior_1), 2).alias("duration_sec_avg_1_prior"),
    )
)

result = (
    with_windows
    .rollup("artist_id", "mood_pos")
    .agg(
        F.round(F.sum("mood_pos") / F.nullif(F.sum("bpm"), F.lit(0)), 3).alias("mood_pos_to_bpm_ratio"),
    )
)
