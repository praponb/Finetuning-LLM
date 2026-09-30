from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type_claim_date_year = Window.partitionBy("claim_type", F.year("claim_date"))
w_claim_type_by_claim_date_tie = Window.partitionBy("claim_type").orderBy("claim_date")
w_claim_type_by_claim_date_ful = Window.partitionBy("claim_type").orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_approved_amount = Window.orderBy(F.col("approved_amount").desc_nulls_first(), "id")
w_status_claim_type_group = Window.partitionBy("status", "claim_type_group")
w_reported_at_month_by_approve = Window.partitionBy(F.date_trunc("month", "reported_at")).orderBy("approved_amount", "id")
w_policy_id_claim_type_by_clai = Window.partitionBy("policy_id", "claim_type").orderBy("claim_date")
w_claim_type_by_claim_date_run = Window.partitionBy("claim_type").orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_reported_at_month_by_approve_2 = Window.partitionBy(F.date_trunc("month", "reported_at")).orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_holder_id_status = Window.partitionBy("policy_holder_id", "status")
w_all_by_approved_amount_adj_s = Window.orderBy("approved_amount_adj_share_of_claim_type_claim_da", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_approved_amount_decile_polic = Window.partitionBy("approved_amount_decile", "policy_product")
w_claim_type_by_policy_end_dat = Window.partitionBy("claim_type").orderBy(F.col("policy_end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_claims
    .filter((F.col("claimed_amount") <= 42838.9) & (F.col("adjuster_id").isNotNull()))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.round(F.col("approved_amount") * 1.5, 2).alias("approved_amount_adj"),
        F.when(F.col("claim_type").isin("fire", "theft"), "group_a").when(F.col("claim_type") == "collision", "group_b").otherwise("other").alias("claim_type_group"),
    )
)

with_details = (
    filtered.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.product").alias("policy_product"),
        F.col("p.end_date").alias("policy_end_date"),
    )
)

metrics = (
    with_details
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "approved_amount_adj",
        "claim_type_group",
        "policy_holder_id",
        "policy_product",
        "policy_end_date",
        F.round(F.col("approved_amount_adj") / F.nullif(F.sum("approved_amount_adj").over(w_claim_type_claim_date_year), F.lit(0)), 4).alias("approved_amount_adj_share_of_claim_type_claim_da"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_claim_type_claim_date_year), F.lit(0)), 4).alias("claimed_amount_share_of_claim_type_claim_date_ye"),
        F.dense_rank().over(w_claim_type_by_claim_date_tie).alias("claim_date_dense_rank"),
        F.last("claimed_amount").over(w_claim_type_by_claim_date_ful).alias("last_claimed_amount"),
        F.ntile(10).over(w_all_by_approved_amount).alias("approved_amount_decile"),
        F.count(F.lit(1)).over(w_status_claim_type_group).alias("rows_in_status_claim_type_group"),
        F.ntile(3).over(w_all_by_approved_amount).alias("approved_amount_tercile"),
        F.sum("approved_amount").over(w_status_claim_type_group).alias("status_claim_type_group_sum_approved_amount"),
        F.lead("approved_amount", 3).over(w_reported_at_month_by_approve).alias("next3_approved_amount"),
        F.round(F.percent_rank().over(w_policy_id_claim_type_by_clai), 3).alias("claim_date_percent_rank"),
        F.count(F.lit(1)).over(w_claim_type_by_claim_date_run).alias("running_count"),
        F.nth_value("approved_amount", 2).over(w_reported_at_month_by_approve_2).alias("second_approved_amount"),
        F.lead("claim_date").over(w_all_by_approved_amount).alias("next_claim_date"),
    )
)

leaders = metrics.filter(F.col("claim_date_dense_rank") == 1)

windowed = (
    leaders
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "approved_amount_adj",
        "claim_type_group",
        "policy_product",
        "policy_end_date",
        "approved_amount_adj_share_of_claim_type_claim_da",
        "claimed_amount_share_of_claim_type_claim_date_ye",
        "claim_date_dense_rank",
        "last_claimed_amount",
        "approved_amount_decile",
        "rows_in_status_claim_type_group",
        "approved_amount_tercile",
        "status_claim_type_group_sum_approved_amount",
        "claim_date_percent_rank",
        "running_count",
        "second_approved_amount",
        "next_claim_date",
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_policy_holder_id_status), 2).alias("claimed_amount_vs_policy_holder_id_status_avg"),
        F.first("running_count").over(w_all_by_approved_amount_adj_s).alias("first_running_count"),
        F.count(F.lit(1)).over(w_approved_amount_decile_polic).alias("rows_in_approved_amount_decile_policy_product"),
        F.nth_value("claimed_amount", 2).over(w_claim_type_by_policy_end_dat).alias("second_claimed_amount"),
    )
)

result = (
    windowed
    .filter(F.col("claimed_amount_vs_policy_holder_id_status_avg") <= 20000.0)
    .select(
        "id",
        "policy_id",
        "claim_date",
        "approved_amount",
        "status",
        "policy_product",
        "policy_end_date",
        "approved_amount_adj_share_of_claim_type_claim_da",
        "claimed_amount_share_of_claim_type_claim_date_ye",
        "claim_date_dense_rank",
        "approved_amount_decile",
        "approved_amount_tercile",
        "status_claim_type_group_sum_approved_amount",
        "claim_date_percent_rank",
        "second_approved_amount",
        "next_claim_date",
        "claimed_amount_vs_policy_holder_id_status_avg",
        "first_running_count",
        "rows_in_approved_amount_decile_policy_product",
        "second_claimed_amount",
        F.concat_ws(" / ", F.col("claim_type"), F.col("claim_type_group")).alias("claim_type_claim_type_group_label"),
        F.when(F.col("approved_amount") >= 17759.3, "HIGH").when(F.col("approved_amount") >= 4868.7, "MEDIUM").otherwise("LOW").alias("approved_amount_tier"),
    )
)
