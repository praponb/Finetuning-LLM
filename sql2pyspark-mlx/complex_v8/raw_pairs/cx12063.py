from pyspark.sql import functions as F

filtered = (
    df_adjusters
    .filter((F.lower(F.col("adjuster_name")).like("%wa%")) & (F.col("level") <= 3))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
    )
)

scored = (
    filtered
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("adjuster_name")), " ", "_")).alias("adjuster_name_slug"),
        F.when(F.col("team").isin("Motor", "Travel"), "group_a").when(F.col("team") == "Health", "group_b").otherwise("other").alias("team_group"),
        F.when(F.col("team").isin("Travel", "Property"), "group_a").when(F.col("team") == "Motor", "group_b").otherwise("other").alias("team_group_2"),
    )
)

with_activity = (
    scored.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.id"), "left_semi")
    .filter(F.col("level") <= 3)
)

exploded_detail = (
    with_activity.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.id")) & (F.col("c.claim_type") == "collision"), "left")
    .select(
        "s.id",
        "s.team",
        "s.level",
        "s.adjuster_name_slug",
        "s.team_group",
        "s.team_group_2",
        F.col("c.id").alias("claim_id"),
        F.col("c.status").alias("claim_status"),
        "c.claim_type",
        F.col("c.policy_id").alias("claim_policy_id"),
    )
)

result = (
    exploded_detail
    .select(
        "id",
        "team",
        "adjuster_name_slug",
        "team_group",
        "team_group_2",
        "claim_id",
        "claim_status",
        "claim_type",
        F.abs(F.col("level") - 3).alias("level_distance"),
        F.abs(F.col("level") - 3).alias("level_distance_2"),
    )
)
