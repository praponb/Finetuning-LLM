from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("artist_id").orderBy(F.col("moods_count").desc(), F.col("id").desc())
w_latest_2 = Window.partitionBy("artist_id").orderBy(F.col("greatest_moods_count_duration_sec").desc(), F.col("id").desc())

prepared_tracks = (
    df_tracks
    .filter((F.col("artist_id") <= 2) & (F.col("duration_sec") < 312))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "moods",
        F.size(F.col("moods")).alias("moods_count"),
    )
)

unique_rows = (
    prepared_tracks
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

scored = (
    unique_rows
    .withColumn("greatest_moods_count_duration_sec", F.greatest(F.col("moods_count"), F.col("duration_sec")))
)

without_matches = (
    scored.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (~F.col("a.is_verified")) & (F.col("a.country") == "JP"), "left_anti")
)

latest = (
    without_matches
    .withColumn("row_num", F.row_number().over(w_latest_2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

qualified = (
    latest.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (~F.array_contains(F.col("t.moods"), "chill")) & (F.lower(F.col("t.title")).like("%he%")), "left_semi")
)

result = (
    qualified
    .filter(F.col("duration_sec") >= 165)
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "moods",
        "moods_count",
        F.when(F.col("moods_count") >= 2, "HIGH").when(F.col("moods_count") >= 1, "MEDIUM").otherwise("LOW").alias("moods_count_tier"),
        F.round(F.col("moods_count") / F.nullif(F.col("duration_sec"), F.lit(0)), 3).alias("moods_count_per_duration_sec"),
    )
    .orderBy("moods_count", F.col("artist_id").asc())
)
