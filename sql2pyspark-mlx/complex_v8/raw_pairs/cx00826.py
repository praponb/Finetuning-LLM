from pyspark.sql import functions as F

recent = (
    df_adjusters
    .filter((F.col("level") >= 3) & (F.upper(F.trim(F.col("adjuster_name"))).like("A%")))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.lower(F.regexp_replace(F.trim(F.col("adjuster_name")), " ", "_")).alias("adjuster_name_slug"),
        F.concat_ws("-", F.col("adjuster_name"), F.col("team")).alias("adjuster_name_team_label"),
    )
)

line_level_claims = (
    recent.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.team",
        "s.adjuster_name_team_label",
        F.col("c.id").alias("claim_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.policy_id").alias("claim_policy_id"),
    )
)

with_activity = (
    line_level_claims.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.id")) & (F.col("c.claimed_amount") > 11235.6), "left_semi")
    .filter(F.col("team") == "Property")
)

result = (
    with_activity
    .select(
        "id",
        "team",
        "adjuster_name_team_label",
        "claim_id",
        "claim_approved_amount",
        "claim_policy_id",
        F.upper(F.trim(F.col("adjuster_name_team_label"))).alias("adjuster_name_team_label_clean"),
        F.length(F.trim(F.col("adjuster_name_team_label"))).alias("adjuster_name_team_label_len"),
    )
)
