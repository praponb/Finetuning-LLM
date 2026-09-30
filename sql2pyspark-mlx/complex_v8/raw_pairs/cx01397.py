from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("artist_id").orderBy(F.col("duration_sec").desc(), F.col("id").desc())

prepared_tracks = (
    df_tracks
    .filter((F.col("bpm") > 94) & (F.col("artist_id") >= 11))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        F.length(F.trim(F.col("title"))).alias("title_len"),
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
    )
)

latest_per_key = (
    prepared_tracks
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

screened = (
    latest_per_key.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.debut_year") > 1997) & (F.col("a.is_verified")), "left_semi")
)

with_activity = (
    screened.alias("s")
    .join(df_tracks.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left_semi")
)

result = (
    with_activity
    .filter(F.col("bpm") > 75)
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "title_len",
        "title_slug",
        (F.col("bpm") + F.col("title_len")).alias("bpm_plus_title_len"),
    )
    .orderBy("title_len")
)
