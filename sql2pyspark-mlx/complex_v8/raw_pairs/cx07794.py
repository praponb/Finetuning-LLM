from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_verified_genre = Window.partitionBy("is_verified", "genre")
w_all_by_total_track_duration = Window.orderBy("total_track_duration_sec", "id").rowsBetween(-6, -1)
w_all_by_total_track_duration_2 = Window.orderBy("total_track_duration_sec")

filtered_artists = (
    df_artists
    .filter((F.col("country") != "JP") & (~F.col("genre").isin("Pop", "Indie")))
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "is_verified",
        F.when(F.col("is_verified"), 1).otherwise(0).alias("is_verified_flag"),
    )
)

country_stats = (
    filtered_artists
    .groupBy("country")
    .agg(F.percentile_approx("is_verified_flag", 0.5).alias("median_is_verified_flag"))
)

vs_group = (
    filtered_artists.alias("s")
    .join(country_stats.alias("g"), F.col("g.country") == F.col("s.country"), "inner")
    .filter(F.col("s.is_verified_flag") >= F.col("g.median_is_verified_flag"))
    .select(
        "s.*",
        "g.median_is_verified_flag",
        F.round(F.col("s.is_verified_flag") - F.col("g.median_is_verified_flag"), 2).alias("is_verified_flag_minus_median"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.min("bpm").alias("min_track_bpm"),
        F.min("release_date").alias("first_track_release_date"),
        F.sum("duration_sec").alias("total_track_duration_sec"),
    )
)

profiled = (
    vs_group.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "t.min_track_bpm",
        "t.first_track_release_date",
        "t.total_track_duration_sec",
    )
)

with_windows = (
    profiled
    .select(
        "id",
        "artist_name",
        "country",
        "is_verified",
        "is_verified_flag_minus_median",
        "min_track_bpm",
        "total_track_duration_sec",
        F.sum("min_track_bpm").over(w_is_verified_genre).alias("is_verified_genre_sum_min_track_bpm"),
        F.max("is_verified_flag").over(w_all_by_total_track_duration).alias("is_verified_flag_max_6_prior"),
        F.count(F.lit(1)).over(w_is_verified_genre).alias("rows_in_is_verified_genre"),
        F.round(F.percent_rank().over(w_all_by_total_track_duration_2), 3).alias("total_track_duration_sec_percent_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "artist_name",
        "country",
        "is_verified",
        "min_track_bpm",
        "total_track_duration_sec",
        "rows_in_is_verified_genre",
        "total_track_duration_sec_percent_rank",
    )
    .orderBy(
        F.col("rows_in_is_verified_genre").desc(),
        F.col("artist_name").asc_nulls_last(),
    )
)
