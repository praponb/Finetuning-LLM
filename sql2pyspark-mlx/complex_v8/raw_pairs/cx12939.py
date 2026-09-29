from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_team_level_by_level_prior_2 = Window.partitionBy("team", "level").orderBy(F.col("level").asc_nulls_last(), "id").rowsBetween(-2, -1)
w_team_level_by_level_full = Window.partitionBy("team", "level").orderBy(F.col("level").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_team_level = Window.partitionBy("team", "level")
w_team_level_by_level = Window.partitionBy("team", "level").orderBy(F.col("level").asc_nulls_last(), "id")

filtered_adjusters = (
    df_adjusters
    .filter(F.col("level").between(1, 3))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.substring(F.trim(F.col("adjuster_name")), 1, 4).alias("adjuster_name_prefix"),
    )
)

metrics = (
    filtered_adjusters
    .select(
        "id",
        "team",
        "level",
        "adjuster_name_prefix",
        F.round(F.avg("level").over(w_team_level_by_level_prior_2), 2).alias("level_avg_2_prior"),
        F.first("level").over(w_team_level_by_level_full).alias("first_level"),
        F.round(F.col("level") - F.avg("level").over(w_team_level), 2).alias("level_vs_team_level_avg"),
        F.round(F.percent_rank().over(w_team_level_by_level), 3).alias("level_percent_rank"),
    )
)

filtered = (
    metrics.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.id"), "left_semi")
)

with_children = (
    filtered.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.team",
        "s.adjuster_name_prefix",
        "s.level_avg_2_prior",
        "s.first_level",
        "s.level_vs_team_level_avg",
        F.col("c.id").alias("claim_id"),
        "c.claim_date",
        F.col("c.policy_id").alias("claim_policy_id"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.reported_at").alias("claim_reported_at"),
    )
)

result = (
    with_children
    .select(
        "id",
        "claim_id",
        "level_avg_2_prior",
        "claim_claimed_amount",
        "level_vs_team_level_avg",
    )
    .unpivot(
        ids=["id", "claim_id"],
        values=["level_avg_2_prior", "claim_claimed_amount", "level_vs_team_level_avg"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
