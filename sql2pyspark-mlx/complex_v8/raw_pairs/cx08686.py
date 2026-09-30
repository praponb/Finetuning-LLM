from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("artist_id").orderBy(F.col("release_date").desc(), F.col("id").desc())

base = df_tracks.filter(F.array_contains(F.col("moods"), "chill"))

latest = (
    base
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

grouped = (
    latest
    .filter(F.col("is_explicit"))
    .groupBy(
        "artist_id",
        "is_explicit",
        F.date_format("release_date", "yyyy-MM").alias("release_date_ym"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.sum("bpm").alias("total_bpm"),
        F.max("duration_sec").alias("max_duration_sec_check"),
    )
    .filter(F.col("max_duration_sec_check") > 209)
    .drop("max_duration_sec_check")
)

result = (
    grouped
    .select(
        "artist_id",
        "is_explicit",
        "release_date_ym",
        "distinct_ids",
        "total_bpm",
    )
    .orderBy("release_date_ym", "artist_id", "is_explicit")
    .limit(10)
)
