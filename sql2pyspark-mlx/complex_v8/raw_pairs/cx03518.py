from pyspark.sql import functions as F

filtered = (
    df_adjusters
    .filter(F.col("level") < 3)
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.substring(F.trim(F.col("adjuster_name")), 1, 4).alias("adjuster_name_prefix"),
        F.abs(F.col("level") - 3).alias("level_distance"),
        F.concat_ws(" / ", F.col("team"), F.col("adjuster_name")).alias("team_adjuster_name_label"),
    )
)

screened = (
    filtered.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.id"), "left_semi")
)

qualified = (
    screened.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.id"), "left_semi")
)

annotated = (
    qualified
    .select(
        "*",
        F.concat_ws(" / ", F.col("team"), F.col("adjuster_name_prefix")).alias("team_adjuster_name_prefix_label"),
        F.concat_ws("-", F.col("team_adjuster_name_label"), F.col("adjuster_name_prefix")).alias("team_adjuster_name_label_adjuster_name_prefix_la"),
        F.length(F.trim(F.col("team"))).alias("team_len"),
    )
)

with_children_claims = (
    annotated.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.level",
        "s.adjuster_name_prefix",
        "s.team_adjuster_name_label",
        "s.team_adjuster_name_label_adjuster_name_prefix_la",
        F.col("c.id").alias("claim_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
        "c.claim_date",
        F.col("c.status").alias("claim_status"),
        "c.claim_type",
    )
)

result = (
    with_children_claims
    .select(
        "id",
        "level",
        "team_adjuster_name_label",
        "team_adjuster_name_label_adjuster_name_prefix_la",
        "claim_id",
        "claim_reported_at",
        "claim_date",
    )
    .orderBy("claim_reported_at", F.col("claim_date").desc_nulls_first())
)
