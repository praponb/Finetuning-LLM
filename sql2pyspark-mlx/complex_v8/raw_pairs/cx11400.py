from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_or_unknown_status = Window.partitionBy("status_or_unknown", "status")
w_status_or_unknown_by_premium = Window.partitionBy("status_or_unknown").orderBy(F.col("premium").desc(), "id")
w_all_by_end_date_ties = Window.orderBy(F.col("end_date").asc_nulls_last())
w_status_or_unknown_by_premium_2 = Window.partitionBy("status_or_unknown").orderBy(F.col("premium").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_status_or_unknown_by = Window.partitionBy("status", "status_or_unknown").orderBy(F.col("premium").desc_nulls_first(), "id")
w_all_by_premium = Window.orderBy(F.col("premium").desc(), "id")
w_all_by_premium_running = Window.orderBy(F.col("premium").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_premium_rank_2_runnin = Window.orderBy(F.col("premium_rank_2").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_premium_rank_2_by_premium_ru = Window.partitionBy("premium_rank_2").orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_end_date_cume_dist_tier_prem = Window.partitionBy("end_date_cume_dist_tier", "premium_rank")
w_all_by_min_premium = Window.orderBy(F.col("min_premium").desc(), "end_date_cume_dist_tier", "premium_rank")
w_all_by_min_premium_ties = Window.orderBy(F.col("min_premium").desc())

base_policys = (
    df_policies
    .filter((F.col("premium") < 2945.9) & (~F.col("status").isin("EXPIRED", "LAPSED")))
    .select(
        "id",
        "holder_id",
        "premium",
        "end_date",
        "status",
        F.concat_ws(" / ", F.col("status"), F.col("product")).alias("status_product_label"),
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
    )
)

with_trends = (
    base_policys
    .select(
        "*",
        F.size(F.collect_set("holder_id").over(w_status_or_unknown_status)).alias("distinct_holder_id_in_status_or_unknown_status"),
        F.rank().over(w_status_or_unknown_by_premium).alias("premium_rank"),
        F.round(F.col("premium") - F.avg("premium").over(w_status_or_unknown_status), 2).alias("premium_vs_status_or_unknown_status_avg"),
        F.round(F.cume_dist().over(w_all_by_end_date_ties), 3).alias("end_date_cume_dist"),
        F.last("premium").over(w_status_or_unknown_by_premium_2).alias("last_premium"),
        F.round(F.percent_rank().over(w_status_status_or_unknown_by), 3).alias("premium_percent_rank"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_status_or_unknown_status), F.lit(0)), 4).alias("premium_share_of_status_or_unknown_status"),
        F.rank().over(w_all_by_premium).alias("premium_rank_2"),
        F.max("premium").over(w_all_by_premium_running).alias("max_premium_so_far"),
    )
)

with_windows = (
    with_trends
    .select(
        "id",
        "premium",
        "status_product_label",
        "status_or_unknown",
        "distinct_holder_id_in_status_or_unknown_status",
        "premium_rank",
        "end_date_cume_dist",
        "last_premium",
        "premium_percent_rank",
        "premium_share_of_status_or_unknown_status",
        "premium_rank_2",
        "max_premium_so_far",
        F.last("end_date_cume_dist", ignorenulls=True).over(w_all_by_premium_rank_2_runnin).alias("last_known_end_date_cume_dist"),
        F.count(F.lit(1)).over(w_status_or_unknown_status).alias("rows_in_status_or_unknown_status"),
        F.round(F.col("premium_rank") - F.avg("premium_rank").over(w_status_or_unknown_status), 2).alias("premium_rank_vs_status_or_unknown_status_avg"),
        F.round(F.col("premium") - F.avg("premium").over(w_status_or_unknown_status), 2).alias("premium_vs_status_or_unknown_status_avg_2"),
        F.count(F.lit(1)).over(w_premium_rank_2_by_premium_ru).alias("running_count"),
    )
)

stats = (
    with_windows
    .groupBy(
        F.when(F.col("end_date_cume_dist") >= 0.6, "HIGH").when(F.col("end_date_cume_dist") >= 0.2, "MEDIUM").otherwise("LOW").alias("end_date_cume_dist_tier"),
        "premium_rank",
    )
    .agg(
        F.sum("premium_share_of_status_or_unknown_status").alias("total_premium_share_of_status_or_unknown_status"),
        F.min("rows_in_status_or_unknown_status").alias("min_rows_in_status_or_unknown_status"),
        F.min("premium").alias("min_premium"),
        F.percentile_approx("running_count", 0.5).alias("median_running_count"),
        F.round(F.stddev("premium_rank_vs_status_or_unknown_status_avg"), 2).alias("stddev_premium_rank_vs_status_or_unknown_status_"),
        F.avg("premium_rank_2").alias("avg_premium_rank_2_check"),
    )
    .filter(F.col("avg_premium_rank_2_check") > 4)
    .drop("avg_premium_rank_2_check")
)

sequenced = (
    stats
    .select(
        "*",
        F.round(F.col("min_premium") - F.avg("min_premium").over(w_end_date_cume_dist_tier_prem), 2).alias("min_premium_vs_end_date_cume_dist_tier_premium_r"),
        F.rank().over(w_all_by_min_premium).alias("min_premium_rank"),
        F.round(F.cume_dist().over(w_all_by_min_premium), 3).alias("min_premium_cume_dist"),
        F.round(F.percent_rank().over(w_all_by_min_premium_ties), 3).alias("min_premium_percent_rank"),
    )
)

result = (
    sequenced
    .select(
        "end_date_cume_dist_tier",
        "min_rows_in_status_or_unknown_status",
        "median_running_count",
        "min_premium_vs_end_date_cume_dist_tier_premium_r",
        "min_premium_rank",
        "min_premium_cume_dist",
        "min_premium_percent_rank",
        F.round(F.col("min_premium") * 0.9, 2).alias("min_premium_adj"),
        F.lower(F.regexp_replace(F.trim(F.col("end_date_cume_dist_tier")), " ", "_")).alias("end_date_cume_dist_tier_slug"),
    )
    .orderBy("min_premium_cume_dist")
)
