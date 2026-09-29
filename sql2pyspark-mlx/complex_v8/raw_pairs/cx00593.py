from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_len_by_end_date_year = Window.partitionBy("status_len").orderBy(F.col("end_date_year").asc_nulls_last(), "id")
w_end_date_month = Window.partitionBy(F.month("end_date"))
w_all_by_start_date = Window.orderBy("start_date", "id")
w_all_by_start_date_decile_ful = Window.orderBy(F.col("start_date_decile").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_start_date_decile_claim_coun = Window.partitionBy("start_date_decile", "claim_count_2")
w_claim_count_end_date_year_mo = Window.partitionBy("claim_count", F.month("end_date_year")).orderBy(F.col("end_date_year").desc(), "id")
w_end_date_year_dense_rank_by = Window.partitionBy("end_date_year_dense_rank").orderBy("prev_start_date", "id")

base_policys = (
    df_policies
    .filter((F.col("holder_id") >= 3) & (F.col("premium") <= 3719.7))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
        F.length(F.trim(F.col("status"))).alias("status_len"),
        F.date_trunc("year", F.col("end_date")).alias("end_date_year"),
        F.date_format(F.col("end_date"), "yyyy-MM").alias("end_date_label"),
    )
)

ranked = (
    base_policys
    .select(
        "id",
        "start_date",
        "end_date_year",
        F.dense_rank().over(w_status_len_by_end_date_year).alias("end_date_year_dense_rank"),
        F.round(F.col("status_len") - F.avg("status_len").over(w_end_date_month), 2).alias("status_len_vs_end_date_month_avg"),
        F.ntile(10).over(w_all_by_start_date).alias("start_date_decile"),
        F.sum("status_len").over(w_end_date_month).alias("end_date_month_sum_status_len"),
        F.lag("start_date").over(w_all_by_start_date).alias("prev_start_date"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count("*").alias("claim_count"),
        F.count("*").alias("claim_count_2"),
        F.sum("approved_amount").alias("total_claim_approved_amount"),
    )
)

with_stats = (
    ranked.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        F.coalesce(F.col("c.claim_count_2"), F.lit(0)).alias("claim_count_2"),
        "c.total_claim_approved_amount",
    )
)

with_trends = (
    with_stats
    .select(
        "*",
        F.last("start_date").over(w_all_by_start_date_decile_ful).alias("last_start_date"),
        F.count(F.lit(1)).over(w_start_date_decile_claim_coun).alias("rows_in_start_date_decile_claim_count_2"),
        F.ntile(5).over(w_claim_count_end_date_year_mo).alias("end_date_year_quintile"),
        F.round(F.col("claim_count_2") / F.nullif(F.sum("claim_count_2").over(w_start_date_decile_claim_coun), F.lit(0)), 4).alias("claim_count_2_share_of_start_date_decile_claim_c"),
        F.round(F.cume_dist().over(w_end_date_year_dense_rank_by), 3).alias("prev_start_date_cume_dist"),
        F.round(F.avg("start_date_decile").over(w_start_date_decile_claim_coun), 2).alias("start_date_decile_claim_count_2_avg_start_date_d"),
    )
)

claim_stats_2 = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.round(F.avg("approved_amount"), 2).alias("avg_claim_approved_amount"),
        F.count(F.when(F.col("claim_type").isNotNull(), 1)).alias("claim_claim_type_count"),
        F.min("approved_amount").alias("min_claim_approved_amount"),
    )
)

summarized_rows = (
    with_trends.alias("s")
    .join(claim_stats_2.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "c.avg_claim_approved_amount",
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
        "c.min_claim_approved_amount",
    )
)

result = (
    summarized_rows
    .filter(F.col("avg_claim_approved_amount") > 10000.0)
    .select(
        "id",
        "start_date",
        "end_date_year_dense_rank",
        "status_len_vs_end_date_month_avg",
        "start_date_decile",
        "end_date_month_sum_status_len",
        "prev_start_date",
        "claim_count_2",
        "total_claim_approved_amount",
        "last_start_date",
        "end_date_year_quintile",
        "claim_count_2_share_of_start_date_decile_claim_c",
        "prev_start_date_cume_dist",
        "start_date_decile_claim_count_2_avg_start_date_d",
        "avg_claim_approved_amount",
        "claim_claim_type_count",
        "min_claim_approved_amount",
        F.trunc(F.col("start_date"), "MM").alias("start_date_month_start"),
        (F.col("claim_count_2") + F.col("end_date_year_dense_rank")).alias("claim_count_2_plus_end_date_year_dense_rank"),
    )
    .orderBy("start_date_decile_claim_count_2_avg_start_date_d")
)
