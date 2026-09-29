from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_reported_at = Window.orderBy("reported_at", "id")
w_approved_amount_tier_claim_t = Window.partitionBy("approved_amount_tier", "claim_type")
w_claim_type_by_claimed_amount = Window.partitionBy("claim_type").orderBy(F.col("claimed_amount").desc(), "id")
w_approved_amount_tier_by_poli = Window.partitionBy("approved_amount_tier").orderBy("policy_premium", "id")
w_status_by_claim_date_full = Window.partitionBy("status").orderBy(F.col("claim_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_approved_amount_tier_by_poli_2 = Window.partitionBy("approved_amount_tier").orderBy("policy_premium")
w_approved_amount_tier_policy = Window.partitionBy("approved_amount_tier", "policy_id")

filtered_claims = (
    df_claims
    .filter((F.col("policy_id").isNotNull()) & (F.col("claim_date").isNotNull()))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        F.when(F.col("status").isin("APPROVED", "REJECTED"), "group_a").when(F.col("status") == "IN_REVIEW", "group_b").otherwise("other").alias("status_group"),
        F.when(F.col("approved_amount") >= 20880.4, "large").when(F.col("approved_amount") >= 0.0, "medium").otherwise("small").alias("approved_amount_tier"),
    )
)

joined_policy = (
    filtered_claims.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.status",
        "s.reported_at",
        "s.status_group",
        "s.approved_amount_tier",
        F.col("p.premium").alias("policy_premium"),
        F.col("p.end_date").alias("policy_end_date"),
    )
)

with_trends = (
    joined_policy
    .select(
        "*",
        F.row_number().over(w_all_by_reported_at).alias("seq_no"),
        F.round(F.col("policy_premium") / F.nullif(F.sum("policy_premium").over(w_approved_amount_tier_claim_t), F.lit(0)), 4).alias("policy_premium_share_of_approved_amount_tier_cla"),
        F.size(F.collect_set("policy_id").over(w_approved_amount_tier_claim_t)).alias("distinct_policy_id_in_approved_amount_tier_claim"),
        F.rank().over(w_claim_type_by_claimed_amount).alias("claimed_amount_rank"),
        F.round(F.avg("claimed_amount").over(w_approved_amount_tier_claim_t), 2).alias("approved_amount_tier_claim_type_avg_claimed_amou"),
        F.dense_rank().over(w_approved_amount_tier_by_poli).alias("policy_premium_dense_rank"),
        F.nth_value("claim_date", 2).over(w_status_by_claim_date_full).alias("second_claim_date"),
        F.size(F.collect_set("status").over(w_approved_amount_tier_claim_t)).alias("distinct_status_in_approved_amount_tier_claim_ty"),
        F.dense_rank().over(w_approved_amount_tier_by_poli_2).alias("policy_premium_dense_rank_2"),
        F.max("policy_premium").over(w_approved_amount_tier_claim_t).alias("approved_amount_tier_claim_type_max_policy_premi"),
        F.min("claimed_amount").over(w_approved_amount_tier_claim_t).alias("approved_amount_tier_claim_type_min_claimed_amou"),
        F.max("claimed_amount").over(w_approved_amount_tier_claim_t).alias("approved_amount_tier_claim_type_max_claimed_amou"),
        F.round(F.col("policy_premium") - F.avg("policy_premium").over(w_approved_amount_tier_policy), 2).alias("policy_premium_vs_approved_amount_tier_policy_id"),
        F.dense_rank().over(w_claim_type_by_claimed_amount).alias("claimed_amount_dense_rank"),
    )
)

derived = (
    with_trends
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        "status_group",
        "policy_end_date",
        "policy_premium_share_of_approved_amount_tier_cla",
        "claimed_amount_rank",
        "policy_premium_dense_rank",
        "second_claim_date",
        "distinct_status_in_approved_amount_tier_claim_ty",
        "approved_amount_tier_claim_type_max_policy_premi",
        "approved_amount_tier_claim_type_min_claimed_amou",
        "policy_premium_vs_approved_amount_tier_policy_id",
        "claimed_amount_dense_rank",
        (F.col("reported_at") + F.expr("INTERVAL 7 DAYS")).alias("reported_at_plus_7d"),
        F.concat_ws("|", F.col("claim_type"), F.col("approved_amount_tier")).alias("claim_type_approved_amount_tier_label"),
    )
)

result = (
    derived
    .filter(F.col("claim_type_approved_amount_tier_label").isNotNull())
    .select(
        "id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        "policy_end_date",
        "policy_premium_share_of_approved_amount_tier_cla",
        "claimed_amount_rank",
        "second_claim_date",
        "distinct_status_in_approved_amount_tier_claim_ty",
        "approved_amount_tier_claim_type_min_claimed_amou",
        "policy_premium_vs_approved_amount_tier_policy_id",
        "claimed_amount_dense_rank",
        "reported_at_plus_7d",
        F.when(F.col("claimed_amount_rank") >= 8, "A").when(F.col("claimed_amount_rank") >= 3, "B").otherwise("C").alias("claimed_amount_rank_tier"),
        F.lower(F.regexp_replace(F.trim(F.col("claim_type")), " ", "_")).alias("claim_type_slug"),
    )
    .orderBy(F.col("approved_amount_tier_claim_type_min_claimed_amou").asc())
)
