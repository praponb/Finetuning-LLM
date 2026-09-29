from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("adjuster_team").orderBy(F.col("adjuster_level").desc(), F.col("id").desc())

valid_claims = (
    df_claims
    .filter((F.col("adjuster_id").isNotNull()) | (F.month(F.col("reported_at")).isin(1, 2)))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        F.quarter(F.col("claim_date")).alias("claim_date_quarter"),
        F.year(F.col("claim_date")).alias("claim_date_year"),
    )
)

enriched_adjuster = (
    valid_claims.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.approved_amount",
        "s.claim_date_year",
        F.col("a.level").alias("adjuster_level"),
        F.col("a.team").alias("adjuster_team"),
        "a.adjuster_name",
    )
)

filtered = (
    enriched_adjuster.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.col("a.level") <= 3), "left_semi")
)

latest = (
    filtered
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_parent_adjuster = (
    latest.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date_year",
        "s.adjuster_level",
        "s.adjuster_team",
        "s.adjuster_name",
        F.col("a.adjuster_name").alias("adjuster_adjuster_name"),
        F.col("a.team").alias("adjuster_team_2"),
        F.col("a.level").alias("adjuster_level_2"),
        F.col("p.start_date").alias("policy_start_date"),
        F.col("p.premium").alias("policy_premium"),
    )
)

result = (
    with_parent_adjuster
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date_year",
        "adjuster_level",
        "adjuster_team",
        "adjuster_name",
        "adjuster_adjuster_name",
        "adjuster_team_2",
        "adjuster_level_2",
        "policy_start_date",
        "policy_premium",
    )
    .orderBy(F.col("adjuster_adjuster_name").desc(), "id")
    .limit(10)
)
