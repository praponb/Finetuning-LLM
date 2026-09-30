from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("artist_id").orderBy(F.col("title_len").desc(), F.col("id").desc())

cleaned = (
    df_tracks
    .filter((F.col("artist_id").isNotNull()) & (~F.col("is_explicit")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.quarter(F.col("release_date")).alias("release_date_quarter"),
        F.length(F.trim(F.col("title"))).alias("title_len"),
    )
)

without_matches = (
    cleaned.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (~F.col("t.is_explicit")) & (F.col("t.release_date") < F.lit("2025-03-08").cast("date")), "left_anti")
)

unique_rows = (
    without_matches
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

annotated = (
    unique_rows
    .select(
        "*",
        F.lower(F.trim(F.col("title"))).alias("title_clean"),
        F.month(F.col("release_date")).alias("release_date_month"),
        F.date_format(F.col("release_date"), "yyyy-MM-dd").alias("release_date_label"),
    )
)

result = (
    annotated
    .groupingSets(
        [["release_date_month", "release_date_quarter"], ["release_date_month"], []],
        "release_date_month", "release_date_quarter",
    )
    .agg(F.round(F.avg("bpm"), 2).alias("avg_bpm"), F.count("*").alias("cnt"))
    .orderBy(
        F.col("release_date_month").asc_nulls_last(),
        F.col("release_date_quarter").asc_nulls_last(),
    )
)
