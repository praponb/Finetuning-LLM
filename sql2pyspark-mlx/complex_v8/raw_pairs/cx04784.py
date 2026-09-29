from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_is_explicit_labe = Window.partitionBy("is_explicit", "is_explicit_label")
w_release_date_month_is_explic = Window.partitionBy(F.month("release_date"), "is_explicit")
w_is_explicit_by_bpm = Window.partitionBy("is_explicit").orderBy(F.col("bpm").desc(), "id")
w_artist_country_by_bpm_full = Window.partitionBy("artist_country").orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_country_by_bpm = Window.partitionBy("artist_country").orderBy("bpm", "id")
w_artist_country_artist_id_by = Window.partitionBy("artist_country", "artist_id").orderBy("bpm", "id").rowsBetween(-6, Window.currentRow)
w_all_by_release_date = Window.orderBy("release_date", "id")
w_all_by_release_date_ties = Window.orderBy("release_date")
w_is_explicit_label_by_distinc = Window.partitionBy("is_explicit_label").orderBy(F.col("distinct_is_explicit_labels").desc_nulls_first(), "is_explicit_label").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_is_explicit_labels = Window.partitionBy("distinct_is_explicit_labels").orderBy("stddev_release_date_month_is_explicit_max_bpm", "is_explicit_label")
w_is_explicit_label_by_distinc_2 = Window.partitionBy("is_explicit_label").orderBy(F.col("distinct_is_explicit_labels").desc_nulls_first(), "is_explicit_label").rowsBetween(-1, -1)

base = (
    df_tracks
    .filter(F.col("bpm") >= 82)
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "bpm",
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label"),
        F.dayofweek(F.col("release_date")).alias("release_date_dayofweek"),
    )
)

combined_details_artist = (
    base.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.release_date",
        "s.is_explicit",
        "s.bpm",
        "s.is_explicit_label",
        F.col("a.country").alias("artist_country"),
    )
)

metrics = (
    combined_details_artist
    .select(
        "id",
        "release_date",
        "is_explicit",
        "bpm",
        "is_explicit_label",
        F.count(F.lit(1)).over(w_is_explicit_is_explicit_labe).alias("rows_in_is_explicit_is_explicit_label"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_release_date_month_is_explic), F.lit(0)), 4).alias("bpm_share_of_release_date_month_is_explicit"),
        F.round(F.percent_rank().over(w_is_explicit_by_bpm), 3).alias("bpm_percent_rank"),
        F.size(F.collect_set("artist_country").over(w_release_date_month_is_explic)).alias("distinct_artist_country_in_release_date_month_is"),
        F.first("bpm").over(w_artist_country_by_bpm_full).alias("first_bpm"),
        (F.col("bpm") - F.lag("bpm").over(w_artist_country_by_bpm)).alias("bpm_change"),
        F.round(F.avg("bpm").over(w_artist_country_artist_id_by), 2).alias("bpm_avg_7"),
        F.rank().over(w_all_by_release_date).alias("release_date_rank"),
        F.max("bpm").over(w_release_date_month_is_explic).alias("release_date_month_is_explicit_max_bpm"),
        F.min("bpm").over(w_release_date_month_is_explic).alias("release_date_month_is_explicit_min_bpm"),
        F.round(F.cume_dist().over(w_all_by_release_date_ties), 3).alias("release_date_cume_dist"),
    )
)

scored = (
    metrics
    .select(
        "id",
        "bpm",
        "is_explicit_label",
        "bpm_percent_rank",
        "distinct_artist_country_in_release_date_month_is",
        "bpm_change",
        "bpm_avg_7",
        "release_date_month_is_explicit_max_bpm",
        "release_date_month_is_explicit_min_bpm",
        "release_date_cume_dist",
        F.concat(F.col("is_explicit_label"), F.lit("#"), F.col("bpm").cast("string")).alias("is_explicit_label_ref"),
    )
)

summary = (
    scored
    .groupBy("is_explicit_label")
    .agg(
        F.countDistinct("is_explicit_label").alias("distinct_is_explicit_labels"),
        F.round(F.stddev("distinct_artist_country_in_release_date_month_is"), 2).alias("stddev_distinct_artist_country_in_release_date_m"),
        F.round(F.stddev("release_date_month_is_explicit_max_bpm"), 2).alias("stddev_release_date_month_is_explicit_max_bpm"),
        F.sum("bpm_percent_rank").alias("sum_bpm_percent_rank_check"),
    )
    .filter(F.col("sum_bpm_percent_rank_check") > 0.2)
    .drop("sum_bpm_percent_rank_check")
)

metrics_2 = (
    summary
    .select(
        "is_explicit_label",
        "stddev_distinct_artist_country_in_release_date_m",
        "stddev_release_date_month_is_explicit_max_bpm",
        F.nth_value("distinct_is_explicit_labels", 2).over(w_is_explicit_label_by_distinc).alias("second_distinct_is_explicit_labels"),
        F.ntile(3).over(w_distinct_is_explicit_labels).alias("stddev_release_date_month_is_explicit_max_bpm_te"),
        F.round(F.avg("distinct_is_explicit_labels").over(w_is_explicit_label_by_distinc_2), 2).alias("distinct_is_explicit_labels_avg_1_prior"),
    )
)

result = (
    metrics_2
    .select("is_explicit_label", "stddev_release_date_month_is_explicit_max_bpm")
    .orderBy(
        "is_explicit_label",
        F.col("stddev_release_date_month_is_explicit_max_bpm").asc(),
    )
)
