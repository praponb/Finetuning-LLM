from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_team_len_level_by_level_prio = Window.partitionBy("team_len", "level").orderBy("level", "id").rowsBetween(-1, -1)
w_team_len_level_by_level_ties = Window.partitionBy("team_len", "level").orderBy("level")

prepared = (
    df_adjusters
    .filter(F.upper(F.trim(F.col("adjuster_name"))).like("A%"))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.length(F.trim(F.col("team"))).alias("team_len"),
    )
)

metrics = (
    prepared
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        "team_len",
        F.sum("team_len").over(w_team_len_level_by_level_prio).alias("team_len_sum_1_prior"),
        F.round(F.percent_rank().over(w_team_len_level_by_level_ties), 3).alias("level_percent_rank"),
    )
)

exploded_detail_claims = (
    metrics.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.team",
        "s.team_len_sum_1_prior",
        "s.level_percent_rank",
        F.col("c.id").alias("claim_id"),
        F.col("c.policy_id").alias("claim_policy_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
    )
)

derived = (
    exploded_detail_claims
    .select(
        "*",
        F.coalesce(F.col("team"), F.lit("UNKNOWN")).alias("team_or_unknown"),
        (F.floor(F.col("claim_approved_amount") / 10000) * 10000).alias("claim_approved_amount_bucket"),
    )
)

result = (
    derived
    .select(
        "id",
        "team",
        "team_len_sum_1_prior",
        "level_percent_rank",
        "claim_id",
        "claim_policy_id",
        "claim_approved_amount",
        "team_or_unknown",
        "claim_approved_amount_bucket",
        F.when(F.col("claim_approved_amount") >= 17759.3, "large").when(F.col("claim_approved_amount") >= 2272.7, "medium").otherwise("small").alias("claim_approved_amount_tier"),
        F.least(F.col("team_len_sum_1_prior"), F.col("claim_approved_amount")).alias("least_team_len_sum_1_prior_claim_approved_amount"),
    )
    .orderBy(F.col("team").desc_nulls_first(), F.col("id").asc_nulls_last())
)
