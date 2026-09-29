from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_is_explicit_false_is_expli = Window.partitionBy("n_is_explicit_false", "is_explicit").orderBy("stddev_bpm")
w_n_is_explicit_false_is_expli_2 = Window.partitionBy("n_is_explicit_false", "is_explicit")
w_n_is_explicit_false_is_expli_3 = Window.partitionBy("n_is_explicit_false", "is_explicit").orderBy("stddev_bpm", "is_explicit")
w_n_is_explicit_false = Window.partitionBy("n_is_explicit_false")
w_n_is_explicit_false_by_n_is = Window.partitionBy("n_is_explicit_false").orderBy(F.col("n_is_explicit_false_is_explicit_min_pct_upper_tr").desc(), "is_explicit")
w_n_is_explicit_false_by_n_is_2 = Window.partitionBy("n_is_explicit_false").orderBy(F.col("n_is_explicit_false_is_explicit_min_pct_upper_tr").desc())
w_all_by_least_stddev_bpm_perc = Window.orderBy(F.col("least_stddev_bpm_percent_rank_stddev_bpm").asc_nulls_last(), "is_explicit")

valid_tracks = (
    df_tracks
    .filter(F.lower(F.col("title")).like("%o %"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        F.year(F.col("release_date")).alias("release_date_year"),
    )
)

rollup_base = (
    valid_tracks
    .groupBy("is_explicit")
    .agg(
        F.sum(F.when(~F.col("is_explicit"), 1).otherwise(0)).alias("n_is_explicit_false"),
        F.sum(F.when(~F.col("is_explicit"), F.col("bpm"))).alias("bpm_not_is_explicit"),
        F.max("release_date").alias("last_release_date"),
        F.round(F.stddev("bpm"), 2).alias("stddev_bpm"),
        F.datediff(F.max("release_date"), F.min("release_date")).alias("release_date_span_days"),
        F.round(100 * F.count(F.when(F.upper(F.trim(F.col("title"))).like("R%"), 1)) / F.count("*"), 1).alias("pct_upper_trim_title_like_r"),
    )
)

sequenced = (
    rollup_base
    .select(
        "is_explicit",
        "n_is_explicit_false",
        "bpm_not_is_explicit",
        "last_release_date",
        "stddev_bpm",
        F.round(F.percent_rank().over(w_n_is_explicit_false_is_expli), 3).alias("stddev_bpm_percent_rank"),
        F.round(F.col("bpm_not_is_explicit") / F.nullif(F.sum("bpm_not_is_explicit").over(w_n_is_explicit_false_is_expli_2), F.lit(0)), 4).alias("bpm_not_is_explicit_share_of_n_is_explicit_false"),
        F.min("pct_upper_trim_title_like_r").over(w_n_is_explicit_false_is_expli_2).alias("n_is_explicit_false_is_explicit_min_pct_upper_tr"),
        F.lag("pct_upper_trim_title_like_r", 2).over(w_n_is_explicit_false_is_expli_3).alias("prev2_pct_upper_trim_title_like_r"),
    )
)

enriched = (
    sequenced
    .filter(F.col("stddev_bpm_percent_rank") > 0.2)
    .select(
        "is_explicit",
        "n_is_explicit_false",
        "bpm_not_is_explicit",
        "stddev_bpm",
        "n_is_explicit_false_is_explicit_min_pct_upper_tr",
        "prev2_pct_upper_trim_title_like_r",
        F.least(F.col("stddev_bpm_percent_rank"), F.col("stddev_bpm")).alias("least_stddev_bpm_percent_rank_stddev_bpm"),
    )
)

sequenced_2 = (
    enriched
    .select(
        "is_explicit",
        "n_is_explicit_false",
        "stddev_bpm",
        "n_is_explicit_false_is_explicit_min_pct_upper_tr",
        "prev2_pct_upper_trim_title_like_r",
        "least_stddev_bpm_percent_rank_stddev_bpm",
        F.ntile(10).over(w_n_is_explicit_false_by_n_is).alias("n_is_explicit_false_is_explicit_min_pct_upper_tr_2"),
        F.round(F.col("stddev_bpm") / F.nullif(F.sum("stddev_bpm").over(w_n_is_explicit_false), F.lit(0)), 4).alias("stddev_bpm_share_of_n_is_explicit_false"),
        F.rank().over(w_n_is_explicit_false_by_n_is_2).alias("n_is_explicit_false_is_explicit_min_pct_upper_tr_3"),
        F.row_number().over(w_n_is_explicit_false_by_n_is).alias("rn"),
        F.count(F.lit(1)).over(w_n_is_explicit_false).alias("rows_in_n_is_explicit_false"),
        (F.col("stddev_bpm") - F.lag("stddev_bpm").over(w_all_by_least_stddev_bpm_perc)).alias("stddev_bpm_change"),
    )
)

filtered = (
    sequenced_2
    .crossJoin(sequenced_2.agg(F.avg("rn").alias("overall_avg_rn")))
    .filter(F.col("rn") > F.col("overall_avg_rn"))
    .drop("overall_avg_rn")
)

top_per_group = filtered.filter((F.col("rn") <= 3) & (F.col("n_is_explicit_false") < 6))

result = (
    top_per_group
    .filter(F.col("n_is_explicit_false_is_explicit_min_pct_upper_tr_3").between(4, 7))
    .select(
        "is_explicit",
        "n_is_explicit_false",
        "stddev_bpm",
        "prev2_pct_upper_trim_title_like_r",
        "least_stddev_bpm_percent_rank_stddev_bpm",
        "n_is_explicit_false_is_explicit_min_pct_upper_tr_2",
        "rows_in_n_is_explicit_false",
        "stddev_bpm_change",
        (F.col("stddev_bpm") + F.col("stddev_bpm_change")).alias("stddev_bpm_plus_stddev_bpm_change"),
        F.abs(F.col("rn") - 4).alias("rn_distance"),
    )
    .orderBy(F.col("rn_distance").desc())
)
