from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("adjuster_id").orderBy(F.col("reported_at").desc(), F.col("id").desc())
w_policy_id_adjuster_id = Window.partitionBy("policy_id", "adjuster_id")

recent = (
    df_claims
    .filter(F.col("claim_type").isNotNull())
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
    )
)

latest_per_key = (
    recent
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

combined_details = (
    latest_per_key.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        F.col("a.team").alias("adjuster_team"),
    )
)

windowed = (
    combined_details
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
        "adjuster_team",
        F.round(F.avg("approved_amount").over(w_policy_id_adjuster_id), 2).alias("policy_id_adjuster_id_avg_approved_amount"),
        F.max("approved_amount").over(w_policy_id_adjuster_id).alias("policy_id_adjuster_id_max_approved_amount"),
        F.count(F.lit(1)).over(w_policy_id_adjuster_id).alias("rows_in_policy_id_adjuster_id"),
    )
)

with_parent_policy = (
    windowed.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        "s.policy_id_adjuster_id_avg_approved_amount",
        F.col("p.start_date").alias("policy_start_date"),
        F.col("po.region").alias("holder_region"),
        F.col("po.risk_score").alias("holder_risk_score"),
    )
)

result = (
    with_parent_policy
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "policy_id_adjuster_id_avg_approved_amount",
        "policy_start_date",
        "holder_region",
        "holder_risk_score",
        F.abs(F.col("claimed_amount") - 29359.4).alias("claimed_amount_distance"),
        F.dayofweek(F.col("claim_date")).alias("claim_date_dayofweek"),
    )
    .orderBy("id")
)
