from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_approved_amount_ties = Window.orderBy(F.col("approved_amount").desc())
w_all_by_approved_amount = Window.orderBy(F.col("approved_amount").desc(), "id")
w_all_by_approved_amount_movin = Window.orderBy(F.col("approved_amount").desc(), "id").rowsBetween(-6, Window.currentRow)
w_policy_holder_id_policy_stat = Window.partitionBy("policy_holder_id", "policy_status")

recent = (
    df_claims
    .filter((F.hour(F.col("reported_at")).between(8, 13)) & (F.col("claim_type") == "medical"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
    )
)

combined_details_policy = (
    recent.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.premium") > 1051.9), "inner")
    .filter(F.col("p.holder_id").isNotNull())
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.approved_amount",
        F.col("p.premium").alias("policy_premium"),
        F.col("p.holder_id").alias("policy_holder_id"),
    )
)

scored = (
    combined_details_policy
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "policy_premium",
        "policy_holder_id",
        (F.col("approved_amount") - F.col("policy_premium")).alias("approved_amount_minus_policy_premium"),
        F.abs(F.col("approved_amount") - 17759.3).alias("approved_amount_distance"),
        F.coalesce(F.col("approved_amount"), F.lit(0.0)).alias("approved_amount_filled"),
    )
)

joined = (
    scored.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .filter(F.col("p.start_date") >= F.lit("2024-07-19").cast("date"))
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.approved_amount",
        "s.policy_premium",
        "s.policy_holder_id",
        "s.approved_amount_distance",
        "s.approved_amount_filled",
        F.col("p.premium").alias("policy_premium_2"),
        F.col("p.start_date").alias("policy_start_date"),
        F.col("p.status").alias("policy_status"),
    )
)

with_details = (
    joined.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.policy_holder_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.approved_amount",
        "s.policy_premium",
        "s.policy_holder_id",
        "s.approved_amount_distance",
        "s.approved_amount_filled",
        "s.policy_premium_2",
        "s.policy_status",
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

metrics = (
    with_details
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "policy_premium",
        "policy_holder_id",
        "approved_amount_distance",
        "policy_premium_2",
        "policy_status",
        "holder_risk_score",
        F.round(F.percent_rank().over(w_all_by_approved_amount_ties), 3).alias("approved_amount_percent_rank"),
        F.row_number().over(w_all_by_approved_amount).alias("rn"),
        F.sum("policy_premium_2").over(w_all_by_approved_amount_movin).alias("policy_premium_2_sum_7"),
        F.count(F.lit(1)).over(w_policy_holder_id_policy_stat).alias("rows_in_policy_holder_id_policy_status"),
    )
)

with_parent_adjuster = (
    metrics.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.policy_premium",
        "s.policy_holder_id",
        "s.approved_amount_distance",
        "s.policy_status",
        "s.holder_risk_score",
        "s.approved_amount_percent_rank",
        "s.rn",
        "s.policy_premium_2_sum_7",
        "s.rows_in_policy_holder_id_policy_status",
        F.col("a.level").alias("adjuster_level"),
        F.col("p.status").alias("policy_status_2"),
        F.col("p.start_date").alias("policy_start_date"),
    )
)

result = (
    with_parent_adjuster
    .select(
        "id",
        "adjuster_id",
        "approved_amount_distance",
        "policy_status",
        "holder_risk_score",
        "approved_amount_percent_rank",
        "rn",
        "policy_premium_2_sum_7",
        "rows_in_policy_holder_id_policy_status",
        "policy_status_2",
        "policy_start_date",
        F.least(F.col("adjuster_level"), F.col("approved_amount_distance")).alias("least_adjuster_level_approved_amount_distance"),
    )
    .orderBy(
        F.col("rows_in_policy_holder_id_policy_status").desc(),
        F.col("holder_risk_score").asc(),
        "id",
    )
    .limit(10)
)
