from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_name_len_2_by_polic = Window.partitionBy("adjuster_name_len_2").orderBy("policy_premium", "id", "claim_id")
w_claim_reported_at_month_by_a = Window.partitionBy(F.month("claim_reported_at")).orderBy("adjuster_name_len_2", "id", "claim_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_adjuster_name_len_2_d = Window.orderBy(F.col("adjuster_name_len_2_distance").desc(), "id", "claim_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_adjuster_name_len_2_d_2 = Window.orderBy(F.col("adjuster_name_len_2_distance").desc(), "id", "claim_id")

base_adjusters = (
    df_adjusters
    .filter((F.col("level") < 3) & (F.col("team") == "Property"))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.lower(F.regexp_replace(F.trim(F.col("adjuster_name")), " ", "_")).alias("adjuster_name_slug"),
        F.length(F.trim(F.col("adjuster_name"))).alias("adjuster_name_len"),
    )
)

line_level_claims = (
    base_adjusters.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.adjuster_name",
        "s.adjuster_name_len",
        F.col("c.id").alias("claim_id"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        "c.claim_type",
        F.col("c.policy_id").alias("claim_policy_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
    )
)

scored = (
    line_level_claims
    .select(
        "*",
        F.length(F.trim(F.col("adjuster_name"))).alias("adjuster_name_len_2"),
        F.round(F.col("claim_approved_amount") * 0.25, 2).alias("claim_approved_amount_adj"),
    )
)

with_parent_claim = (
    scored.alias("s")
    .join(df_claims.alias("c"), F.col("c.id") == F.col("s.claim_id"), "inner")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.claim_policy_id"), "inner")
    .select(
        "s.id",
        "s.adjuster_name",
        "s.adjuster_name_len",
        "s.claim_id",
        "s.claim_claimed_amount",
        "s.claim_type",
        "s.claim_policy_id",
        "s.adjuster_name_len_2",
        "s.claim_approved_amount_adj",
        F.col("c.approved_amount").alias("claim_approved_amount_2"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("p.premium").alias("policy_premium"),
    )
)

labeled = (
    with_parent_claim
    .withColumn("adjuster_name_len_2_distance", F.abs(F.col("adjuster_name_len_2") - 8))
)

metrics = (
    labeled
    .select(
        "*",
        F.round(F.cume_dist().over(w_adjuster_name_len_2_by_polic), 3).alias("policy_premium_cume_dist"),
        F.nth_value("adjuster_name_len", 2).over(w_claim_reported_at_month_by_a).alias("second_adjuster_name_len"),
        F.last("claim_claimed_amount", ignorenulls=True).over(w_all_by_adjuster_name_len_2_d).alias("last_known_claim_claimed_amount"),
        F.row_number().over(w_all_by_adjuster_name_len_2_d_2).alias("adjuster_name_len_2_distance_seq"),
    )
)

result = (
    metrics
    .filter(F.col("adjuster_name_len") >= 6)
    .select(
        "id",
        "adjuster_name",
        "claim_id",
        "claim_claimed_amount",
        "claim_type",
        "claim_policy_id",
        "adjuster_name_len_2",
        "claim_approved_amount_adj",
        "claim_reported_at",
        "policy_premium",
        "policy_premium_cume_dist",
    )
    .orderBy(F.col("claim_type").desc())
)
