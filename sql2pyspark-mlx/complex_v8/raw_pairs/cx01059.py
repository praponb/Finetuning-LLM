from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("adjuster_team").orderBy("claim_date", "id")

scoped_claims = (
    df_claims
    .filter(F.col("claimed_amount") <= 44991.5)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        F.concat_ws("-", F.col("claim_type"), F.col("status")).alias("claim_type_status_label"),
        (F.floor(F.col("claimed_amount") / 10000) * 10000).alias("claimed_amount_bucket"),
        F.date_trunc("week", F.col("claim_date")).alias("claim_date_week"),
    )
)

combined_details_policy = (
    scoped_claims.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.approved_amount",
        "s.status",
        "s.claim_type_status_label",
        "s.claimed_amount_bucket",
        "s.claim_date_week",
        F.col("p.status").alias("policy_status"),
        "a.adjuster_name",
        F.col("a.team").alias("adjuster_team"),
    )
)

unique_rows = (
    combined_details_policy
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

labeled = (
    unique_rows
    .select(
        "*",
        F.abs(F.col("approved_amount") - 14993.0).alias("approved_amount_distance"),
        F.when(F.col("adjuster_team").isin("Health", "Motor"), "group_a").when(F.col("adjuster_team") == "Property", "group_b").otherwise("other").alias("adjuster_team_group"),
    )
)

result = (
    labeled
    .filter(F.col("claim_type_status_label").isNotNull())
    .select(
        "id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "status",
        "claim_type_status_label",
        "claimed_amount_bucket",
        "claim_date_week",
        "policy_status",
        "adjuster_name",
        "adjuster_team",
        "approved_amount_distance",
        "adjuster_team_group",
    )
    .orderBy(F.col("claim_date_week").desc(), "adjuster_name", "id")
    .limit(5)
)
