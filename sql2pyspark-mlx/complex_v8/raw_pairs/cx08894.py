from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("artist_id").orderBy(F.col("release_date").desc(), F.col("id").desc())

valid = (
    df_tracks
    .filter((F.col("is_explicit")) & (F.col("title").isNotNull()))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        F.coalesce(F.col("bpm"), F.lit(0)).alias("bpm_filled"),
        F.array_join(F.array_sort(F.col("moods")), ",").alias("moods_text"),
        F.concat(F.col("title"), F.lit("#"), F.col("bpm").cast("string")).alias("title_ref"),
    )
)

tagged = (
    valid
    .withColumn("moods_text_title_ref_label", F.concat_ws(" / ", F.col("moods_text"), F.col("title_ref")))
)

latest = (
    tagged
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .filter(F.col("bpm_filled") > 108)
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "bpm",
        "bpm_filled",
        "moods_text",
        F.abs(F.col("bpm_filled") - 102).alias("bpm_filled_distance"),
    )
    .orderBy("id")
)
