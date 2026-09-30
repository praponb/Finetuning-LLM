from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("artist_id").orderBy(F.col("duration_sec").desc(), F.col("id").desc())

filtered = (
    df_tracks
    .filter((F.size(F.col("moods")) >= 1) & (F.col("release_date") < F.lit("2024-12-03").cast("date")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "moods",
        F.substring(F.trim(F.col("title")), 1, 4).alias("title_prefix"),
        F.greatest(F.col("duration_sec"), F.col("bpm")).alias("greatest_duration_sec_bpm"),
        F.month(F.col("release_date")).alias("release_date_month"),
    )
)

screened = (
    filtered.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.col("t.duration_sec") < 352), "left_anti")
)

latest_per_key = (
    screened
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest_per_key
    .filter(F.col("duration_sec") > 184)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "moods",
        "greatest_duration_sec_bpm",
        "release_date_month",
        (F.col("duration_sec") - F.col("release_date_month")).alias("duration_sec_minus_release_date_month"),
        (F.floor(F.col("duration_sec") / 50) * 50).alias("duration_sec_bucket"),
    )
    .orderBy(F.col("title").desc(), F.col("duration_sec").desc(), "id")
    .limit(20)
)
