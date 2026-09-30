from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_policy_end_date = Window.orderBy("policy_end_date", "id")
w_reported_at_to_date_adjuster = Window.partitionBy(F.to_date("reported_at"), "adjuster_level")
w_adjuster_id_reported_at_mont = Window.partitionBy("adjuster_id", F.date_trunc("month", "reported_at"))
w_policy_end_date_year_policy = Window.partitionBy(F.year("policy_end_date"), "policy_status").orderBy(F.col("reported_at").desc(), "id")
w_reported_at_month_adjuster_i = Window.partitionBy(F.date_trunc("month", "reported_at"), "adjuster_id").orderBy("claimed_amount", "id")
w_reported_at_month_adjuster_i_2 = Window.partitionBy(F.date_trunc("month", "reported_at"), "adjuster_id").orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_end_date_year_policy_2 = Window.partitionBy(F.year("policy_end_date"), "policy_status").orderBy(F.col("reported_at").desc())
w_policy_end_date_year_policy_3 = Window.partitionBy(F.year("policy_end_date"), "policy_status").orderBy(F.col("reported_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_policy_end_date_full = Window.orderBy("policy_end_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claim_type_by_adjuster_level = Window.partitionBy("claim_type").orderBy(F.col("adjuster_level").desc(), "id").rowsBetween(-1, -1)
w_adjuster_id_reported_at_quar = Window.partitionBy("adjuster_id", "reported_at_quartile")
w_distinct_status_in_reported = Window.partitionBy("distinct_status_in_reported_at_to_date_adjuster_", "policy_id")
w_adjuster_level_claim_type_by = Window.partitionBy("adjuster_level", "claim_type").orderBy(F.col("reported_at_quartile").desc_nulls_first(), "id")
w_adjuster_level_claim_type_by_2 = Window.partitionBy("adjuster_level", "claim_type").orderBy(F.col("reported_at_quartile").desc_nulls_first(), "id").rowsBetween(-6, -1)
w_adjuster_level_by_reported_a = Window.partitionBy("adjuster_level").orderBy("reported_at_quartile", "id")
w_reported_at_to_date_reported = Window.partitionBy(F.to_date("reported_at"), "reported_at_quartile").orderBy("claimed_amount_vs_adjuster_id_reported_at_month_", "id")

cleaned = (
    df_claims
    .filter((F.col("claim_date") >= F.lit("2024-08-07").cast("date")) | (F.col("claimed_amount") < 44921.3))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
    )
)

with_details_adjuster = (
    cleaned.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.*",
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.status").alias("policy_status"),
    )
)

with_trends = (
    with_details_adjuster
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_policy_end_date), 3).alias("policy_end_date_percent_rank"),
        F.sum("adjuster_level").over(w_reported_at_to_date_adjuster).alias("reported_at_to_date_adjuster_level_sum_adjuster_"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_adjuster_id_reported_at_mont), 2).alias("claimed_amount_vs_adjuster_id_reported_at_month_"),
        F.ntile(4).over(w_policy_end_date_year_policy).alias("reported_at_quartile"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_reported_at_to_date_adjuster), 2).alias("claimed_amount_vs_reported_at_to_date_adjuster_l"),
        F.lag("policy_end_date").over(w_reported_at_month_adjuster_i).alias("prev_policy_end_date"),
        F.last("reported_at").over(w_reported_at_month_adjuster_i_2).alias("last_reported_at"),
        F.round(F.percent_rank().over(w_policy_end_date_year_policy_2), 3).alias("reported_at_percent_rank"),
        F.count(F.lit(1)).over(w_reported_at_to_date_adjuster).alias("rows_in_reported_at_to_date_adjuster_level"),
        F.round(F.col("adjuster_level") / F.nullif(F.sum("adjuster_level").over(w_reported_at_to_date_adjuster), F.lit(0)), 4).alias("adjuster_level_share_of_reported_at_to_date_adju"),
        F.first("policy_end_date", ignorenulls=True).over(w_policy_end_date_year_policy_3).alias("first_known_policy_end_date"),
        F.round(F.cume_dist().over(w_policy_end_date_year_policy), 3).alias("reported_at_cume_dist"),
        F.size(F.collect_set("status").over(w_reported_at_to_date_adjuster)).alias("distinct_status_in_reported_at_to_date_adjuster_"),
        F.first("claimed_amount").over(w_all_by_policy_end_date_full).alias("first_claimed_amount"),
        F.max("claimed_amount").over(w_claim_type_by_adjuster_level).alias("claimed_amount_max_1_prior"),
    )
)

ranked = (
    with_trends
    .select(
        "*",
        F.round(F.col("claimed_amount_max_1_prior") - F.avg("claimed_amount_max_1_prior").over(w_adjuster_id_reported_at_quar), 2).alias("claimed_amount_max_1_prior_vs_adjuster_id_report"),
        F.size(F.collect_set("claim_type").over(w_distinct_status_in_reported)).alias("distinct_claim_type_in_distinct_status_in_report"),
        F.max("first_claimed_amount").over(w_distinct_status_in_reported).alias("distinct_status_in_reported_at_to_date_adjuster__2"),
        F.ntile(3).over(w_adjuster_level_claim_type_by).alias("reported_at_quartile_tercile"),
        F.round(F.col("reported_at_cume_dist") / F.nullif(F.sum("reported_at_cume_dist").over(w_adjuster_id_reported_at_quar), F.lit(0)), 4).alias("reported_at_cume_dist_share_of_adjuster_id_repor"),
        F.round(F.avg("policy_end_date_percent_rank").over(w_adjuster_level_claim_type_by_2), 2).alias("policy_end_date_percent_rank_avg_6_prior"),
        F.lag("reported_at_quartile").over(w_adjuster_level_by_reported_a).alias("prev_reported_at_quartile"),
        F.lag("policy_end_date", 2).over(w_reported_at_to_date_reported).alias("prev2_policy_end_date"),
    )
)

result = (
    ranked
    .filter(F.col("status").isin("IN_REVIEW", "PAID", "REJECTED"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        "adjuster_level",
        "policy_end_date",
        "policy_status",
        "policy_end_date_percent_rank",
        "claimed_amount_vs_adjuster_id_reported_at_month_",
        "reported_at_quartile",
        "claimed_amount_vs_reported_at_to_date_adjuster_l",
        "prev_policy_end_date",
        "reported_at_percent_rank",
        "adjuster_level_share_of_reported_at_to_date_adju",
        "first_known_policy_end_date",
        "reported_at_cume_dist",
        "distinct_status_in_reported_at_to_date_adjuster_",
        "reported_at_quartile_tercile",
        "policy_end_date_percent_rank_avg_6_prior",
        "prev_reported_at_quartile",
        "prev2_policy_end_date",
        F.date_format(F.col("last_reported_at"), "yyyy-MM-dd HH:00").alias("last_reported_at_label"),
        (F.col("adjuster_level") - F.col("reported_at_percent_rank")).alias("adjuster_level_minus_reported_at_percent_rank"),
    )
    .orderBy(F.col("policy_status").desc_nulls_first(), "status")
)
