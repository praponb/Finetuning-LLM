from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_title_len_by_release_date_mo = Window.partitionBy("title_len").orderBy(F.col("release_date_month").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_title_len_by_release_date_mo_2 = Window.partitionBy("title_len").orderBy(F.col("release_date_month").desc_nulls_first(), "id").rowsBetween(-6, -1)
w_release_date_year = Window.partitionBy(F.year("release_date"))
w_title_len_by_release_date_mo_3 = Window.partitionBy("title_len").orderBy(F.col("release_date_month").desc_nulls_first(), "id")
w_latest = Window.partitionBy("artist_id").orderBy("release_date_month_cume_dist", "id")

prepared_tracks = (
    df_tracks
    .filter((F.col("bpm") < 166) | (~F.array_contains(F.col("moods"), "chill")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        F.length(F.trim(F.col("title"))).alias("title_len"),
        F.date_trunc("month", F.col("release_date")).alias("release_date_month"),
        F.substring(F.trim(F.col("title")), 1, 4).alias("title_prefix"),
    )
)

joined_artist = (
    prepared_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.title_len",
        "s.release_date_month",
        "a.artist_name",
    )
)

windowed = (
    joined_artist
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "title_len",
        "artist_name",
        F.min("duration_sec").over(w_title_len_by_release_date_mo).alias("min_duration_sec_so_far"),
        F.round(F.avg("duration_sec").over(w_title_len_by_release_date_mo_2), 2).alias("duration_sec_avg_6_prior"),
        F.count(F.lit(1)).over(w_release_date_year).alias("rows_in_release_date_year"),
        F.round(F.cume_dist().over(w_title_len_by_release_date_mo_3), 3).alias("release_date_month_cume_dist"),
        F.dense_rank().over(w_title_len_by_release_date_mo_3).alias("release_date_month_dense_rank"),
    )
)

latest_per_key = (
    windowed
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest_per_key
    .cube("artist_id", "release_date_month_dense_rank")
    .agg(
        F.grouping("artist_id").alias("is_total_artist_id"),
        F.round(F.avg("release_date_month_cume_dist"), 2).alias("avg_release_date_month_cume_dist"),
    )
    .orderBy(
        F.col("artist_id").asc_nulls_last(),
        F.col("release_date_month_dense_rank").asc_nulls_last(),
    )
)
