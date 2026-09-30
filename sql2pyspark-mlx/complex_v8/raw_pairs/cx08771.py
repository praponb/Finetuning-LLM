from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_release_date_year = Window.partitionBy("artist_id", F.year("release_date"))
w_artist_id_by_bpm = Window.partitionBy("artist_id").orderBy(F.col("bpm").asc_nulls_last(), "id")
w_release_date_month_artist_id = Window.partitionBy(F.month("release_date"), "artist_id").orderBy(F.col("bpm").asc_nulls_last())
w_dedup = Window.partitionBy("artist_id").orderBy(F.col("rows_in_artist_id_release_date_year").desc(), F.col("id").desc())

base_tracks = (
    df_tracks
    .filter((F.lower(F.col("title")).like("%ur%")) & (F.col("release_date") >= F.lit("2024-10-23").cast("date")))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "bpm",
    )
)

with_windows = (
    base_tracks
    .select(
        "id",
        "artist_id",
        "release_date",
        "bpm",
        F.count(F.lit(1)).over(w_artist_id_release_date_year).alias("rows_in_artist_id_release_date_year"),
        F.row_number().over(w_artist_id_by_bpm).alias("rn"),
        F.rank().over(w_release_date_month_artist_id).alias("bpm_rank"),
        F.round(F.avg("bpm").over(w_artist_id_release_date_year), 2).alias("artist_id_release_date_year_avg_bpm"),
    )
)

latest_per_key = (
    with_windows
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest_per_key
    .select(
        "id",
        "artist_id",
        "release_date",
        "bpm",
        "rows_in_artist_id_release_date_year",
        "rn",
        "bpm_rank",
        F.quarter(F.col("release_date")).alias("release_date_quarter"),
        F.round(F.col("rn") / F.nullif(F.col("bpm_rank"), F.lit(0)), 3).alias("rn_per_bpm_rank"),
    )
    .orderBy(
        F.col("rows_in_artist_id_release_date_year").desc(),
        F.col("rn_per_bpm_rank").desc(),
    )
)
