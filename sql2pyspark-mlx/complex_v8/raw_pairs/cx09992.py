from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("artist_id").orderBy(F.col("duration_sec_filled").desc(), F.col("id").desc())

cleaned = (
    df_tracks
    .filter((F.col("title").isNotNull()) | (F.col("bpm") > 104))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        F.date_format(F.col("release_date"), "yyyy-MM").alias("release_date_label"),
    )
)

annotated = (
    cleaned
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        F.coalesce(F.col("duration_sec"), F.lit(0)).alias("duration_sec_filled"),
        F.coalesce(F.col("duration_sec"), F.lit(0)).alias("duration_sec_filled_2"),
        F.length(F.trim(F.col("title"))).alias("title_len"),
    )
)

latest_per_key = (
    annotated
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest_per_key
    .select(
        "id",
        "title",
        "duration_sec",
        "is_explicit",
        "duration_sec_filled",
        "duration_sec_filled_2",
    )
    .orderBy("duration_sec_filled", "id")
)
