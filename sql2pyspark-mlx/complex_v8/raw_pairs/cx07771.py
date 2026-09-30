from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_start_date_month_by_c = Window.partitionBy(F.date_trunc("month", "policy_start_date")).orderBy(F.col("claim_date").desc(), "id")
w_policy_start_date_month_by_c_2 = Window.partitionBy(F.date_trunc("month", "policy_start_date")).orderBy(F.col("claim_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_end_date_year_policy = Window.partitionBy(F.year("policy_end_date"), F.year("policy_start_date"))
w_adjuster_id_claimed_amount_t = Window.partitionBy("adjuster_id", "claimed_amount_tier").orderBy(F.col("approved_amount").desc(), "id").rowsBetween(-2, Window.currentRow)
w_adjuster_id_claimed_amount_t_2 = Window.partitionBy("adjuster_id", "claimed_amount_tier").orderBy(F.col("approved_amount").desc(), "id")
w_policy_start_date_month_by_c_3 = Window.partitionBy(F.date_trunc("month", "policy_start_date")).orderBy(F.col("claim_date").desc())
w_policy_start_date_month_by_c_4 = Window.partitionBy(F.date_trunc("month", "policy_start_date")).orderBy(F.col("claim_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claimed_amount_tier_policy_s = Window.partitionBy("claimed_amount_tier", "policy_status").orderBy(F.col("policy_start_date").asc_nulls_last(), "id")
w_status = Window.partitionBy("status")
w_adjuster_id_policy_status_by = Window.partitionBy("adjuster_id", "policy_status").orderBy(F.col("claimed_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_adjuster_id_in_poli = Window.partitionBy("distinct_adjuster_id_in_policy_end_date_year_pol").orderBy(F.col("seq_no").desc(), "id")

scoped_claims = (
    df_claims
    .filter((F.col("claim_type") != "collision") | (F.col("claimed_amount") <= 41878.5))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.when(F.col("claimed_amount") >= 31715.7, "large").when(F.col("claimed_amount") >= 19609.8, "medium").otherwise("small").alias("claimed_amount_tier"),
        (F.floor(F.col("claimed_amount") / 10000) * 10000).alias("claimed_amount_bucket"),
    )
)

with_parent = (
    scoped_claims.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.*",
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.status").alias("policy_status"),
        F.col("p.start_date").alias("policy_start_date"),
    )
)

with_windows = (
    with_parent
    .select(
        "*",
        F.ntile(5).over(w_policy_start_date_month_by_c).alias("claim_date_quintile"),
        F.last("policy_start_date", ignorenulls=True).over(w_policy_start_date_month_by_c_2).alias("last_known_policy_start_date"),
        F.size(F.collect_set("claimed_amount_tier").over(w_policy_end_date_year_policy)).alias("distinct_claimed_amount_tier_in_policy_end_date_"),
        F.last("claim_date", ignorenulls=True).over(w_policy_start_date_month_by_c_2).alias("last_known_claim_date"),
        F.sum("claimed_amount").over(w_adjuster_id_claimed_amount_t).alias("claimed_amount_sum_3"),
        F.ntile(5).over(w_adjuster_id_claimed_amount_t_2).alias("approved_amount_quintile"),
        F.round(F.cume_dist().over(w_policy_start_date_month_by_c), 3).alias("claim_date_cume_dist"),
        F.round(F.avg("claimed_amount").over(w_policy_end_date_year_policy), 2).alias("policy_end_date_year_policy_start_date_year_avg_"),
        F.round(F.cume_dist().over(w_policy_start_date_month_by_c_3), 3).alias("claim_date_cume_dist_2"),
        F.sum("claimed_amount_bucket").over(w_policy_end_date_year_policy).alias("policy_end_date_year_policy_start_date_year_sum_"),
        F.last("claimed_amount").over(w_policy_start_date_month_by_c_4).alias("last_claimed_amount"),
        F.row_number().over(w_claimed_amount_tier_policy_s).alias("seq_no"),
        F.size(F.collect_set("adjuster_id").over(w_policy_end_date_year_policy)).alias("distinct_adjuster_id_in_policy_end_date_year_pol"),
    )
)

top_per_group = with_windows.filter(F.col("seq_no") == 1)

with_windows_2 = (
    top_per_group
    .select(
        "*",
        F.round(F.avg("claimed_amount").over(w_status), 2).alias("status_avg_claimed_amount"),
        F.last("seq_no").over(w_adjuster_id_policy_status_by).alias("last_seq_no"),
        F.row_number().over(w_distinct_adjuster_id_in_poli).alias("seq_no_2"),
    )
)

result = (
    with_windows_2
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        "claimed_amount_tier",
        "claimed_amount_bucket",
        "policy_end_date",
        "policy_status",
        "policy_start_date",
        "claimed_amount_sum_3",
        "approved_amount_quintile",
        "claim_date_cume_dist",
        "claim_date_cume_dist_2",
        "policy_end_date_year_policy_start_date_year_sum_",
        "last_claimed_amount",
        "seq_no",
        "status_avg_claimed_amount",
        "seq_no_2",
        F.concat(F.col("claimed_amount_tier"), F.lit("#"), F.col("adjuster_id").cast("string")).alias("claimed_amount_tier_ref"),
        F.abs(F.col("claimed_amount_bucket") - 15070).alias("claimed_amount_bucket_distance"),
    )
    .orderBy(
        F.col("adjuster_id").desc(),
        F.col("claimed_amount").asc_nulls_last(),
        "id",
    )
    .limit(5)
)
