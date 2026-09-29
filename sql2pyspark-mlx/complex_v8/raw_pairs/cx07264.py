from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_title_len = Window.partitionBy("artist_id", "title_len")
w_all_by_title_len = Window.orderBy(F.col("title_len").desc(), "id")

base_tracks = (
    df_tracks
    .filter(F.col("duration_sec") <= 392)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.coalesce(F.col("bpm"), F.lit(0)).alias("bpm_filled"),
        F.year(F.col("release_date")).alias("release_date_year"),
        F.length(F.trim(F.col("title"))).alias("title_len"),
    )
)

windowed = (
    base_tracks
    .withColumn("rows_in_artist_id_title_len", F.count(F.lit(1)).over(w_artist_id_title_len))
    .withColumn("next_duration_sec", F.lead("duration_sec").over(w_all_by_title_len))
)

result = (
    windowed
    .groupingSets(
        [["release_date_year", "title_len"], ["release_date_year"], []],
        "release_date_year", "title_len",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
