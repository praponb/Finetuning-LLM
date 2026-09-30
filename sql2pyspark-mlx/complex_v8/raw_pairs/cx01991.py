from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_level_team_by_level = Window.partitionBy("level", "team").orderBy(F.col("level").desc(), "id")
w_level_team_by_level_ties = Window.partitionBy("level", "team").orderBy(F.col("level").desc())
w_level = Window.partitionBy("level")
w_team_level_by_level_vs_level = Window.partitionBy("team", "level").orderBy(F.col("level_vs_level_avg").desc(), "id").rowsBetween(-6, -1)
w_next_level_by_level_vs_level = Window.partitionBy("next_level").orderBy("level_vs_level_avg", "id").rowsBetween(-1, -1)

valid = (
    df_adjusters
    .filter(F.col("level") <= 3)
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
    )
)

windowed = (
    valid
    .select(
        "*",
        F.lead("level").over(w_level_team_by_level).alias("next_level"),
        F.round(F.percent_rank().over(w_level_team_by_level_ties), 3).alias("level_percent_rank"),
        F.round(F.col("level") - F.avg("level").over(w_level), 2).alias("level_vs_level_avg"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.count("*").alias("claim_count"),
        F.min("claimed_amount").alias("min_claim_claimed_amount"),
    )
)

profiled = (
    windowed.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        "c.min_claim_claimed_amount",
    )
)

sequenced = (
    profiled
    .select(
        "id",
        "adjuster_name",
        "team",
        "next_level",
        "level_percent_rank",
        "level_vs_level_avg",
        "claim_count",
        "min_claim_claimed_amount",
        F.size(F.collect_set("team").over(w_level)).alias("distinct_team_in_level"),
        F.count(F.lit(1)).over(w_level).alias("rows_in_level"),
        F.sum("min_claim_claimed_amount").over(w_team_level_by_level_vs_level).alias("min_claim_claimed_amount_sum_6_prior"),
        F.sum("next_level").over(w_next_level_by_level_vs_level).alias("next_level_sum_1_prior"),
        F.max("min_claim_claimed_amount").over(w_level).alias("level_max_min_claim_claimed_amount"),
        F.round(F.avg("min_claim_claimed_amount").over(w_level), 2).alias("level_avg_min_claim_claimed_amount"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "adjuster_name",
        "level_percent_rank",
        "level_vs_level_avg",
        "claim_count",
        "min_claim_claimed_amount",
        "rows_in_level",
        "min_claim_claimed_amount_sum_6_prior",
        "level_max_min_claim_claimed_amount",
        "level_avg_min_claim_claimed_amount",
        (F.col("next_level_sum_1_prior") - F.col("level_vs_level_avg")).alias("next_level_sum_1_prior_minus_level_vs_level_avg"),
    )
    .orderBy(F.col("level_vs_level_avg").desc())
)
