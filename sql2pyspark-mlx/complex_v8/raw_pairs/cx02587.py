from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_team_level_by_level_prior_1 = Window.partitionBy("team", "level").orderBy(F.col("level").desc_nulls_first(), "id").rowsBetween(-1, -1)
w_level_team_by_level = Window.partitionBy("level", "team").orderBy(F.col("level").desc(), "id")
w_level_team = Window.partitionBy("level", "team")

filtered_adjusters = (
    df_adjusters
    .filter(F.lower(F.col("adjuster_name")).like("%ar%"))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
    )
)

windowed = (
    filtered_adjusters
    .withColumn("level_sum_1_prior", F.sum("level").over(w_team_level_by_level_prior_1))
    .withColumn("level_cume_dist", F.round(F.cume_dist().over(w_level_team_by_level), 3))
)

with_trends = (
    windowed
    .select(
        "*",
        F.round(F.col("level_cume_dist") - F.avg("level_cume_dist").over(w_level_team), 2).alias("level_cume_dist_vs_level_team_avg"),
        F.round(F.col("level_sum_1_prior") - F.avg("level_sum_1_prior").over(w_level_team), 2).alias("level_sum_1_prior_vs_level_team_avg"),
        F.round(F.col("level") - F.avg("level").over(w_level_team), 2).alias("level_vs_level_team_avg"),
        F.round(F.col("level_sum_1_prior") / F.nullif(F.sum("level_sum_1_prior").over(w_level_team), F.lit(0)), 4).alias("level_sum_1_prior_share_of_level_team"),
    )
)

team_stats = (
    with_trends
    .groupBy("team")
    .agg(F.max("level").alias("max_level"))
)

compared = (
    with_trends.alias("s")
    .join(team_stats.alias("g"), F.col("g.team") == F.col("s.team"), "inner")
    .filter(F.col("s.level") == F.col("g.max_level"))
    .select(
        "s.*",
        "g.max_level",
        F.round(F.col("s.level") - F.col("g.max_level"), 2).alias("level_minus_max"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(F.sum("approved_amount").alias("total_claim_approved_amount"))
)

profiled = (
    compared.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.id"), "left")
    .select("s.*", "c.total_claim_approved_amount")
)

result = (
    profiled
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        "level_sum_1_prior",
        "level_cume_dist",
        "level_sum_1_prior_vs_level_team_avg",
        "level_minus_max",
        "total_claim_approved_amount",
        F.coalesce(F.col("team"), F.lit("UNKNOWN")).alias("team_or_unknown"),
    )
    .orderBy("level_sum_1_prior_vs_level_team_avg", F.col("id").desc())
)
