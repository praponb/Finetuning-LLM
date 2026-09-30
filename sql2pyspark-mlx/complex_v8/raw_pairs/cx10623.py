from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("artist_id").orderBy("release_date_year", "id")

filtered = (
    df_tracks
    .filter(F.col("is_explicit"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        F.date_trunc("year", F.col("release_date")).alias("release_date_year"),
    )
)

unique_rows = (
    filtered
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

artist_id_stats = (
    unique_rows
    .groupBy("artist_id")
    .agg(F.avg("duration_sec").alias("avg_duration_sec"))
)

outliers = (
    unique_rows.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.duration_sec") > F.col("g.avg_duration_sec"))
    .select(
        "s.*",
        "g.avg_duration_sec",
        F.round(F.col("s.duration_sec") - F.col("g.avg_duration_sec"), 2).alias("duration_sec_minus_avg"),
    )
)

qualified = (
    outliers.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.col("t.bpm") < 155), "left_anti")
    .filter(F.col("avg_duration_sec") > 139.5)
)

with_activity = (
    qualified.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.year(F.col("t.release_date")) == 2025), "left_anti")
    .filter(F.col("avg_duration_sec") >= 205.5)
)

result = (
    with_activity
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "release_date_year",
        "avg_duration_sec",
        "duration_sec_minus_avg",
    )
    .orderBy("duration_sec_minus_avg", F.col("id").desc())
)
