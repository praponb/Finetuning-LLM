from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_quality_is_not_null_distan = Window.partitionBy("n_quality_is_not_null_distance", "n_quality_not_in_bad_est").orderBy(F.col("n_quality_not_in_bad_est_filled").desc(), "quality")
w_n_quality_is_not_null = Window.partitionBy("n_quality_is_not_null")
w_n_quality_is_not_null_distan_2 = Window.partitionBy("n_quality_is_not_null_distance", "n_quality_not_in_bad_est").orderBy(F.col("n_quality_not_in_bad_est_filled").desc(), "quality").rowsBetween(-6, -1)

base = (
    df_meter_readings
    .filter(F.col("kwh") <= 85.7)
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
    )
)

enriched = (
    base
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
        (F.floor(F.col("kwh") / 25) * 25).alias("kwh_bucket"),
        (F.floor(F.col("voltage") / 5) * 5).alias("voltage_bucket"),
    )
)

grouped = (
    enriched
    .groupBy("quality")
    .agg(
        F.sum(F.when(~F.col("quality").isin("BAD", "EST"), 1).otherwise(0)).alias("n_quality_not_in_bad_est"),
        F.count(F.when(F.col("quality").isNotNull(), 1)).alias("n_quality_is_not_null"),
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
        F.sum("voltage").alias("sum_voltage_check"),
    )
    .filter(F.col("sum_voltage_check") > 218.2)
    .drop("sum_voltage_check")
)

enriched_2 = (
    grouped
    .select(
        "*",
        (F.floor(F.col("n_quality_is_not_null") / 2) * 2).alias("n_quality_is_not_null_bucket"),
        F.coalesce(F.col("n_quality_not_in_bad_est"), F.lit(0)).alias("n_quality_not_in_bad_est_filled"),
        F.abs(F.col("n_quality_is_not_null") - 6).alias("n_quality_is_not_null_distance"),
    )
)

screened = (
    enriched_2
    .crossJoin(enriched_2.agg(F.min("n_quality_not_in_bad_est_filled").alias("overall_min_n_quality_not_in_bad_est_filled")))
    .filter(F.col("n_quality_not_in_bad_est_filled") > F.col("overall_min_n_quality_not_in_bad_est_filled"))
    .drop("overall_min_n_quality_not_in_bad_est_filled")
)

metrics = (
    screened
    .select(
        "quality",
        "n_quality_not_in_bad_est",
        "n_quality_is_not_null",
        "quality_list",
        "n_quality_is_not_null_bucket",
        "n_quality_not_in_bad_est_filled",
        F.rank().over(w_n_quality_is_not_null_distan).alias("n_quality_not_in_bad_est_filled_rank"),
        F.round(F.col("n_quality_not_in_bad_est") / F.nullif(F.sum("n_quality_not_in_bad_est").over(w_n_quality_is_not_null), F.lit(0)), 4).alias("n_quality_not_in_bad_est_share_of_n_quality_is_n"),
        F.lag("n_quality_is_not_null", 3, 0).over(w_n_quality_is_not_null_distan).alias("prev3_n_quality_is_not_null"),
        F.sum("n_quality_is_not_null_bucket").over(w_n_quality_is_not_null_distan_2).alias("n_quality_is_not_null_bucket_sum_6_prior"),
    )
)

top_per_group = (
    metrics
    .filter(F.col("n_quality_not_in_bad_est_filled_rank") == 1)
    .drop("n_quality_not_in_bad_est_filled_rank")
)

result = (
    top_per_group
    .rollup("prev3_n_quality_is_not_null", "n_quality_not_in_bad_est_filled", "quality")
    .agg(
        F.grouping("prev3_n_quality_is_not_null").alias("is_total_prev3_n_quality_is_not_null"),
        F.sum(F.when(F.col("quality") != "EST", F.col("n_quality_not_in_bad_est")).otherwise(0)).alias("n_quality_not_in_bad_est_quality_est"),
        F.round(F.sum("n_quality_is_not_null_bucket") / F.nullif(F.sum("n_quality_is_not_null_bucket_sum_6_prior"), F.lit(0)), 3).alias("n_quality_is_not_null_bucket_to_n_quality_is_not"),
    )
)
