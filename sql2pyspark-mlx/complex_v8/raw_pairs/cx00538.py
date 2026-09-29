from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("artist_id").orderBy("bpm_bucket", "id")

base = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
        (F.floor(F.col("bpm") / 25) * 25).alias("bpm_bucket"),
        (F.floor(F.col("bpm") / 25) * 25).alias("bpm_bucket_2"),
    )
)

scored = (
    base
    .select(
        "*",
        F.concat_ws(": ", F.col("title_slug"), F.col("title")).alias("title_slug_title_label"),
        (F.col("bpm_bucket") - F.col("bpm_bucket_2")).alias("bpm_bucket_minus_bpm_bucket_2"),
    )
)

unique_rows = (
    scored
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    unique_rows
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "moods",
        "title_slug",
        "bpm_bucket",
        "bpm_bucket_2",
        "title_slug_title_label",
        F.concat_ws("-", F.col("title"), F.col("title_slug_title_label")).alias("title_title_slug_title_label_label"),
    )
    .orderBy("bpm_bucket", "id")
    .limit(10)
)
