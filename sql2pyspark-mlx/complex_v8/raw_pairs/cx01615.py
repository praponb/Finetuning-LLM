from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_genre_by_bpm_moving_1 = Window.partitionBy("artist_genre").orderBy(F.col("bpm").asc_nulls_last(), "id").rowsBetween(-1, Window.currentRow)
w_artist_is_verified_artist_id = Window.partitionBy("artist_is_verified", "artist_id").orderBy(F.col("duration_sec").desc())
w_artist_country_artist_is_ver = Window.partitionBy("artist_country", "artist_is_verified")
w_artist_id_artist_country_by = Window.partitionBy("artist_id", "artist_country").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_genre_by_bpm = Window.partitionBy("artist_genre").orderBy(F.col("bpm").asc_nulls_last(), "id")
w_artist_is_verified_artist_id_2 = Window.partitionBy("artist_is_verified", "artist_id").orderBy(F.col("duration_sec").desc(), "id")
w_artist_is_verified = Window.partitionBy("artist_is_verified")
w_all_by_bpm_ties = Window.orderBy(F.col("bpm").desc())

cleaned_tracks = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
    )
)

with_details = (
    cleaned_tracks.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.upper(F.trim(F.col("a.artist_name"))).like("S%")), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.bpm",
        "s.moods",
        F.col("a.genre").alias("artist_genre"),
        F.col("a.country").alias("artist_country"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

with_trends = (
    with_details
    .select(
        "*",
        F.round(F.avg("duration_sec").over(w_artist_genre_by_bpm_moving_1), 2).alias("duration_sec_avg_2"),
        F.round(F.cume_dist().over(w_artist_is_verified_artist_id), 3).alias("duration_sec_cume_dist"),
        F.count(F.lit(1)).over(w_artist_country_artist_is_ver).alias("rows_in_artist_country_artist_is_verified"),
        F.last("duration_sec").over(w_artist_id_artist_country_by).alias("last_duration_sec"),
        (F.col("duration_sec") - F.lag("duration_sec").over(w_artist_genre_by_bpm)).alias("duration_sec_change"),
        F.round(F.avg("duration_sec").over(w_artist_country_artist_is_ver), 2).alias("artist_country_artist_is_verified_avg_duration_s"),
        F.lead("duration_sec", 3, 0).over(w_artist_is_verified_artist_id_2).alias("next3_duration_sec"),
        F.nth_value("bpm", 2).over(w_artist_id_artist_country_by).alias("second_bpm"),
        F.row_number().over(w_artist_is_verified_artist_id_2).alias("rn"),
        F.count(F.lit(1)).over(w_artist_is_verified).alias("rows_in_artist_is_verified"),
        F.rank().over(w_all_by_bpm_ties).alias("bpm_rank"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_artist_is_verified), 2).alias("duration_sec_vs_artist_is_verified_avg"),
    )
)

tagged = (
    with_trends
    .filter(F.col("bpm_rank") >= 2)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "moods",
        "duration_sec_avg_2",
        "rows_in_artist_country_artist_is_verified",
        "last_duration_sec",
        "duration_sec_change",
        "next3_duration_sec",
        "rn",
        "rows_in_artist_is_verified",
        "bpm_rank",
        "duration_sec_vs_artist_is_verified_avg",
        F.lower(F.trim(F.col("title"))).alias("title_clean"),
    )
)

totals = (
    tagged
    .groupBy(
        "artist_id",
        F.when(F.col("duration_sec_change") >= 126, "HIGH").when(F.col("duration_sec_change") >= -210, "MEDIUM").otherwise("LOW").alias("duration_sec_change_tier"),
        "bpm_rank",
    )
    .agg(
        F.min("bpm_rank").alias("min_bpm_rank"),
        F.countDistinct("artist_id").alias("distinct_artist_ids"),
        F.count("*").alias("n_rows"),
    )
)

result = (
    totals
    .select(
        "artist_id",
        "duration_sec_change_tier",
        "bpm_rank",
        "n_rows",
    )
    .orderBy(F.col("artist_id").desc())
)
