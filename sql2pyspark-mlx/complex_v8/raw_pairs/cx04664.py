from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_release_date = Window.orderBy(F.col("release_date").desc(), "id")
w_release_date_year = Window.partitionBy(F.year("release_date"))
w_artist_id_is_explicit_by_dur = Window.partitionBy("artist_id", "is_explicit").orderBy(F.col("duration_sec").desc(), "id").rowsBetween(-2, Window.currentRow)
w_all_by_release_date_prior_6 = Window.orderBy(F.col("release_date").desc(), "id").rowsBetween(-6, -1)
w_all_by_release_date_moving_3 = Window.orderBy(F.col("release_date").desc(), "id").rowsBetween(-3, Window.currentRow)
w_moods_count = Window.partitionBy("moods_count")
w_is_explicit_by_distinct_arti = Window.partitionBy("is_explicit").orderBy(F.col("distinct_artist_country_in_release_date_year").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_artist_country_in_r = Window.partitionBy("distinct_artist_country_in_release_date_year")

prepared_tracks = (
    df_tracks
    .filter(F.col("bpm") > 100)
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
    )
)

joined = (
    prepared_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        F.col("a.country").alias("artist_country"),
        "a.artist_name",
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

windowed = (
    joined
    .select(
        "id",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        "artist_country",
        "artist_name",
        "artist_is_verified",
        F.ntile(10).over(w_all_by_release_date).alias("release_date_decile"),
        F.round(F.percent_rank().over(w_all_by_release_date), 3).alias("release_date_percent_rank"),
        F.size(F.collect_set("artist_country").over(w_release_date_year)).alias("distinct_artist_country_in_release_date_year"),
        F.round(F.avg("bpm").over(w_release_date_year), 2).alias("release_date_year_avg_bpm"),
        F.max("duration_sec").over(w_artist_id_is_explicit_by_dur).alias("duration_sec_max_3"),
        F.round(F.avg("duration_sec").over(w_release_date_year), 2).alias("release_date_year_avg_duration_sec"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_release_date_year), F.lit(0)), 4).alias("duration_sec_share_of_release_date_year"),
        F.sum("bpm").over(w_release_date_year).alias("release_date_year_sum_bpm"),
        F.sum("bpm").over(w_all_by_release_date_prior_6).alias("bpm_sum_6_prior"),
        F.round(F.avg("duration_sec").over(w_all_by_release_date_moving_3), 2).alias("duration_sec_avg_4"),
    )
)

enriched = windowed.withColumn("moods_count", F.size(F.col("moods")))

ranked = (
    enriched
    .select(
        "*",
        F.round(F.col("release_date_percent_rank") - F.avg("release_date_percent_rank").over(w_moods_count), 2).alias("release_date_percent_rank_vs_moods_count_avg"),
        F.first("release_date_percent_rank", ignorenulls=True).over(w_is_explicit_by_distinct_arti).alias("first_known_release_date_percent_rank"),
        F.sum("release_date_percent_rank").over(w_distinct_artist_country_in_r).alias("distinct_artist_country_in_release_date_year_sum"),
    )
)

totals = (
    ranked
    .filter(~F.col("is_explicit"))
    .groupBy(
        "distinct_artist_country_in_release_date_year",
        F.date_format("release_date", "yyyy-MM").alias("release_date_ym"),
        "moods_count",
    )
    .agg(
        F.sum("release_date_percent_rank").alias("total_release_date_percent_rank"),
        F.array_sort(F.collect_set("artist_name")).alias("artist_name_list"),
        F.avg("release_date_year_avg_bpm").alias("avg_release_date_year_avg_bpm_check"),
    )
    .filter(F.col("avg_release_date_year_avg_bpm_check") > 102.0)
    .drop("avg_release_date_year_avg_bpm_check")
)

result = (
    totals
    .filter(F.col("moods_count") < 2)
    .select(
        "distinct_artist_country_in_release_date_year",
        "release_date_ym",
        "total_release_date_percent_rank",
        "artist_name_list",
        F.when(F.col("moods_count") >= 2, "A").when(F.col("moods_count") >= 1, "B").otherwise("C").alias("moods_count_tier"),
    )
)
