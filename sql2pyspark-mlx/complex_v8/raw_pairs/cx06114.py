from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_by_duration_sec = Window.partitionBy("is_explicit").orderBy(F.col("duration_sec").desc(), "id")
w_is_explicit_by_duration_sec_2 = Window.partitionBy("is_explicit").orderBy(F.col("duration_sec").desc(), "id").rowsBetween(-6, Window.currentRow)
w_is_explicit_artist_is_verifi = Window.partitionBy("is_explicit", "artist_is_verified")
w_artist_is_verified_artist_id = Window.partitionBy("artist_is_verified", "artist_id")
w_artist_is_verified_by_bpm = Window.partitionBy("artist_is_verified").orderBy(F.col("bpm").desc(), "id")
w_is_explicit_by_duration_sec_3 = Window.partitionBy("is_explicit").orderBy(F.col("duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_ids_n_not_is_explic = Window.partitionBy("distinct_ids", "n_not_is_explicit").orderBy(F.col("n_not_is_explicit").desc(), "artist_is_verified")
w_artist_is_verified_by_cnt_mo = Window.partitionBy("artist_is_verified").orderBy("cnt", "artist_is_verified").rowsBetween(-6, Window.currentRow)
w_artist_is_verified_by_cnt_ru = Window.partitionBy("artist_is_verified").orderBy("cnt", "artist_is_verified").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_is_verified_by_cnt_pr = Window.partitionBy("artist_is_verified").orderBy("cnt", "artist_is_verified").rowsBetween(-6, -1)

base_tracks = (
    df_tracks
    .filter((F.col("release_date").between(F.lit("2024-05-12").cast("date"), F.lit("2025-08-28").cast("date"))) | (F.col("duration_sec") > 239))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        (F.col("duration_sec") + F.col("bpm")).alias("duration_sec_plus_bpm"),
        F.date_format(F.col("release_date"), "MMM yyyy").alias("release_date_label"),
    )
)

with_parent = (
    base_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.*",
        "a.artist_name",
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

with_windows = (
    with_parent
    .select(
        "*",
        (F.col("duration_sec_plus_bpm") - F.lag("duration_sec_plus_bpm").over(w_is_explicit_by_duration_sec)).alias("duration_sec_plus_bpm_change"),
        F.max("duration_sec_plus_bpm").over(w_is_explicit_by_duration_sec_2).alias("duration_sec_plus_bpm_max_7"),
        F.size(F.collect_set("artist_id").over(w_is_explicit_artist_is_verifi)).alias("distinct_artist_id_in_is_explicit_artist_is_veri"),
        F.round(F.avg("duration_sec").over(w_artist_is_verified_artist_id), 2).alias("artist_is_verified_artist_id_avg_duration_sec"),
        F.ntile(5).over(w_artist_is_verified_by_bpm).alias("bpm_quintile"),
        F.min("duration_sec_plus_bpm").over(w_artist_is_verified_artist_id).alias("artist_is_verified_artist_id_min_duration_sec_pl"),
        F.sum("duration_sec").over(w_artist_is_verified_artist_id).alias("artist_is_verified_artist_id_sum_duration_sec"),
        F.sum("duration_sec_plus_bpm").over(w_is_explicit_by_duration_sec_3).alias("running_duration_sec_plus_bpm"),
        F.sum("duration_sec").over(w_is_explicit_artist_is_verifi).alias("is_explicit_artist_is_verified_sum_duration_sec"),
    )
)

derived = (
    with_windows
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        "duration_sec_plus_bpm",
        "release_date_label",
        "artist_name",
        "artist_is_verified",
        "duration_sec_plus_bpm_change",
        "duration_sec_plus_bpm_max_7",
        "artist_is_verified_artist_id_avg_duration_sec",
        "artist_is_verified_artist_id_min_duration_sec_pl",
        "artist_is_verified_artist_id_sum_duration_sec",
        "running_duration_sec_plus_bpm",
        (F.floor(F.col("bpm") / 25) * 25).alias("bpm_bucket"),
        (F.col("duration_sec") - F.col("bpm")).alias("duration_sec_minus_bpm"),
    )
)

totals = (
    derived
    .groupBy("artist_is_verified")
    .agg(
        F.count("*").alias("cnt"),
        F.max("duration_sec").alias("max_duration_sec"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(~F.col("is_explicit"), 1).otherwise(0)).alias("n_not_is_explicit"),
        F.array_sort(F.collect_set("artist_name")).alias("artist_name_list"),
        F.avg("artist_is_verified_artist_id_avg_duration_sec").alias("avg_artist_is_verified_artist_id_avg_duration_se"),
    )
    .filter(F.col("avg_artist_is_verified_artist_id_avg_duration_se") > 172.5)
    .drop("avg_artist_is_verified_artist_id_avg_duration_se")
)

ranked = (
    totals
    .select(
        "*",
        (F.col("n_not_is_explicit") - F.lag("n_not_is_explicit").over(w_distinct_ids_n_not_is_explic)).alias("n_not_is_explicit_change"),
        F.round(F.avg("max_duration_sec").over(w_artist_is_verified_by_cnt_mo), 2).alias("max_duration_sec_avg_7"),
        F.first("max_duration_sec", ignorenulls=True).over(w_artist_is_verified_by_cnt_ru).alias("first_known_max_duration_sec"),
        F.sum("n_not_is_explicit").over(w_artist_is_verified_by_cnt_pr).alias("n_not_is_explicit_sum_6_prior"),
    )
)

result = (
    ranked
    .select(
        "artist_is_verified",
        "cnt",
        "max_duration_sec",
        "n_not_is_explicit",
        "n_not_is_explicit_change",
        "first_known_max_duration_sec",
        "n_not_is_explicit_sum_6_prior",
    )
    .orderBy(F.col("cnt").asc_nulls_last())
)
