from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_by_claimed_amount = Window.partitionBy("policy_id").orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id_by_claimed_amount_2 = Window.partitionBy("policy_id").orderBy("claimed_amount", "id").rowsBetween(-6, -1)
w_policy_id_by_claimed_amount_3 = Window.partitionBy("policy_id").orderBy("claimed_amount", "id").rowsBetween(-3, Window.currentRow)
w_policy_id_by_claimed_amount_4 = Window.partitionBy("policy_id").orderBy("claimed_amount", "id")
w_claim_type_group_policy_id = Window.partitionBy("claim_type_group", "policy_id")
w_distinct_adjuster_id_in_clai = Window.partitionBy("distinct_adjuster_id_in_claim_type_group_policy_", "claim_type_len")
w_distinct_adjuster_id_in_clai_2 = Window.partitionBy("distinct_adjuster_id_in_claim_type_group_policy_").orderBy("claim_date", "id")
w_distinct_adjuster_id_in_clai_3 = Window.partitionBy("distinct_adjuster_id_in_claim_type_group_policy_").orderBy("claim_date")
w_distinct_adjuster_id_in_clai_4 = Window.partitionBy("distinct_adjuster_id_in_claim_type_group_policy_").orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_claims = (
    df_claims
    .filter(F.col("claim_type") == "theft")
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.substring(F.trim(F.col("status")), 1, 3).alias("status_prefix"),
        F.when(F.col("claim_type").isin("collision", "fire"), "group_a").when(F.col("claim_type") == "theft", "group_b").otherwise("other").alias("claim_type_group"),
        F.date_trunc("day", F.col("reported_at")).alias("reported_at_day"),
    )
)

ranked = (
    filtered_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "reported_at",
        "reported_at_day",
        F.last("claimed_amount", ignorenulls=True).over(w_policy_id_by_claimed_amount).alias("last_known_claimed_amount"),
        F.round(F.avg("claimed_amount").over(w_policy_id_by_claimed_amount_2), 2).alias("claimed_amount_avg_6_prior"),
        F.count(F.lit(1)).over(w_policy_id_by_claimed_amount).alias("running_count"),
        F.round(F.avg("approved_amount").over(w_policy_id_by_claimed_amount_3), 2).alias("approved_amount_avg_4"),
        (F.col("claimed_amount") - F.lag("claimed_amount").over(w_policy_id_by_claimed_amount_4)).alias("claimed_amount_change"),
        F.size(F.collect_set("adjuster_id").over(w_claim_type_group_policy_id)).alias("distinct_adjuster_id_in_claim_type_group_policy_"),
    )
)

labeled = (
    ranked
    .select(
        "*",
        F.length(F.trim(F.col("claim_type"))).alias("claim_type_len"),
        F.coalesce(F.col("running_count"), F.lit(0)).alias("running_count_filled"),
    )
)

with_windows = (
    labeled
    .select(
        "*",
        F.sum("running_count_filled").over(w_distinct_adjuster_id_in_clai).alias("distinct_adjuster_id_in_claim_type_group_policy__2"),
        F.round(F.cume_dist().over(w_distinct_adjuster_id_in_clai_2), 3).alias("claim_date_cume_dist"),
        F.round(F.percent_rank().over(w_distinct_adjuster_id_in_clai_3), 3).alias("claim_date_percent_rank"),
        F.last("approved_amount_avg_4").over(w_distinct_adjuster_id_in_clai_4).alias("last_approved_amount_avg_4"),
    )
)

with_details_policy = (
    with_windows.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .filter(F.col("p.end_date").isNotNull())
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.reported_at_day",
        "s.claimed_amount_avg_6_prior",
        "s.running_count",
        "s.approved_amount_avg_4",
        "s.claimed_amount_change",
        "s.distinct_adjuster_id_in_claim_type_group_policy_",
        "s.claim_type_len",
        "s.running_count_filled",
        "s.distinct_adjuster_id_in_claim_type_group_policy__2",
        "s.claim_date_cume_dist",
        "s.claim_date_percent_rank",
        F.col("p.end_date").alias("policy_end_date"),
    )
)

derived = (
    with_details_policy
    .filter(F.col("claim_type_len") >= 6)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "running_count",
        "approved_amount_avg_4",
        "claim_type_len",
        "distinct_adjuster_id_in_claim_type_group_policy__2",
        "claim_date_cume_dist",
        "claim_date_percent_rank",
        "policy_end_date",
        F.date_format(F.col("policy_end_date"), "yyyy-MM").alias("policy_end_date_label"),
        F.coalesce(F.col("running_count_filled"), F.lit(0)).alias("running_count_filled_filled"),
        F.quarter(F.col("claim_date")).alias("claim_date_quarter"),
    )
)

tagged = (
    derived
    .filter(F.col("policy_end_date_label").isNotNull())
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "running_count",
        "approved_amount_avg_4",
        "claim_type_len",
        "claim_date_percent_rank",
        "policy_end_date",
        "policy_end_date_label",
        F.concat(F.col("policy_end_date_label"), F.lit("#"), F.col("claim_date_quarter").cast("string")).alias("policy_end_date_label_ref"),
    )
)

result = (
    tagged
    .filter(F.col("policy_end_date_label_ref").isNotNull())
    .select(
        "id",
        "claimed_amount",
        "approved_amount_avg_4",
        "claim_date_percent_rank",
        "policy_end_date_label",
        "policy_end_date_label_ref",
        F.date_trunc("month", F.col("claim_date")).alias("claim_date_month"),
    )
    .orderBy("claimed_amount", F.col("policy_end_date_label_ref").desc_nulls_first())
)
