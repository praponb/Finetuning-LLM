from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_by_release_date = Window.partitionBy("is_explicit").orderBy(F.col("release_date").desc(), "id").rowsBetween(-6, Window.currentRow)
w_artist_id_is_explicit = Window.partitionBy("artist_id", "is_explicit")
w_artist_id_release_date_month = Window.partitionBy("artist_id", F.date_trunc("month", "release_date"))
w_artist_id = Window.partitionBy("artist_id")
w_is_explicit_by_release_date_2 = Window.partitionBy("is_explicit").orderBy(F.col("release_date").desc(), "id")
w_all_by_release_date = Window.orderBy("release_date", "id")
w_is_explicit_artist_id_by_rel = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("release_date").desc(), "id")
w_is_explicit_by_release_date_3 = Window.partitionBy("is_explicit").orderBy(F.col("release_date").desc())
w_artist_id_is_explicit_by_bpm = Window.partitionBy("artist_id", "is_explicit").orderBy("bpm", "id")
w_release_date_quartile_by_row = Window.partitionBy("release_date_quartile").orderBy(F.col("rows_in_artist_id_is_explicit_length_trim_title_").asc_nulls_last())
w_release_date_quartile = Window.partitionBy("release_date_quartile")

prepared = (
    df_tracks
    .filter((F.col("artist_id").isNotNull()) | (F.size(F.col("moods")) >= 2))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.substring(F.trim(F.col("title")), 1, 2).alias("title_prefix"),
    )
)

ranked = (
    prepared
    .select(
        "id",
        "title",
        "release_date",
        "moods",
        "title_prefix",
        F.round(F.avg("bpm").over(w_is_explicit_by_release_date), 2).alias("bpm_avg_7"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_artist_id_is_explicit), 2).alias("duration_sec_vs_artist_id_is_explicit_avg"),
        F.sum("bpm").over(w_is_explicit_by_release_date).alias("bpm_sum_7"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_artist_id_is_explicit), F.lit(0)), 4).alias("duration_sec_share_of_artist_id_is_explicit"),
        F.count(F.lit(1)).over(w_artist_id_is_explicit).alias("rows_in_artist_id_is_explicit"),
        F.max("duration_sec").over(w_artist_id_release_date_month).alias("artist_id_release_date_month_max_duration_sec"),
        F.round(F.avg("bpm").over(w_artist_id), 2).alias("artist_id_avg_bpm"),
        F.round(F.percent_rank().over(w_is_explicit_by_release_date_2), 3).alias("release_date_percent_rank"),
        F.round(F.avg("duration_sec").over(w_artist_id_is_explicit), 2).alias("artist_id_is_explicit_avg_duration_sec"),
        F.round(F.cume_dist().over(w_all_by_release_date), 3).alias("release_date_cume_dist"),
        F.ntile(4).over(w_is_explicit_artist_id_by_rel).alias("release_date_quartile"),
        F.rank().over(w_is_explicit_by_release_date_3).alias("release_date_rank"),
        F.dense_rank().over(w_is_explicit_by_release_date_2).alias("release_date_dense_rank"),
        F.round(F.cume_dist().over(w_artist_id_is_explicit_by_bpm), 3).alias("bpm_cume_dist"),
        F.datediff(F.col("release_date"), F.lag("release_date").over(w_all_by_release_date)).alias("days_since_prev_release_date"),
    )
)

totals = (
    ranked
    .groupBy("release_date_quartile")
    .agg(
        F.sum(F.when(F.length(F.trim(F.col("title"))) >= 8, F.col("rows_in_artist_id_is_explicit"))).alias("rows_in_artist_id_is_explicit_length_trim_title_"),
        F.array_sort(F.collect_set("title")).alias("title_list"),
        F.sum("bpm_avg_7").alias("total_bpm_avg_7"),
        F.max("duration_sec_vs_artist_id_is_explicit_avg").alias("max_duration_sec_vs_artist_id_is_explicit_avg_ch"),
    )
    .filter(F.col("max_duration_sec_vs_artist_id_is_explicit_avg_ch") > -294.0)
    .drop("max_duration_sec_vs_artist_id_is_explicit_avg_ch")
)

sequenced = (
    totals
    .select(
        "*",
        F.rank().over(w_release_date_quartile_by_row).alias("rows_in_artist_id_is_explicit_length_trim_title__2"),
        F.min("total_bpm_avg_7").over(w_release_date_quartile).alias("release_date_quartile_min_total_bpm_avg_7"),
    )
)

result = (
    sequenced
    .filter(F.col("total_bpm_avg_7") >= 468.0)
    .select(
        "release_date_quartile",
        "rows_in_artist_id_is_explicit_length_trim_title_",
        "title_list",
        "total_bpm_avg_7",
        "rows_in_artist_id_is_explicit_length_trim_title__2",
    )
    .orderBy(F.col("total_bpm_avg_7").desc())
)
