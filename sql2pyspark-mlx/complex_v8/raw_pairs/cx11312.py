from pyspark.sql import functions as F

scoped_adjusters = (
    df_adjusters
    .filter((F.col("level") < 3) & (F.length(F.trim(F.col("adjuster_name"))) >= 4))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.when(F.col("team").isin("Property", "Travel"), "group_a").when(F.col("team") == "Health", "group_b").otherwise("other").alias("team_group"),
        F.concat(F.col("adjuster_name"), F.lit("#"), F.col("id").cast("string")).alias("adjuster_name_ref"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(F.min("reported_at").alias("first_claim_reported_at"))
)

with_stats = (
    scoped_adjusters.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.id"), "left")
    .select("s.*", "c.first_claim_reported_at")
)

team_group_stats = (
    with_stats
    .groupBy("team_group")
    .agg(F.avg("level").alias("avg_level"))
)

compared = (
    with_stats.alias("s")
    .join(team_group_stats.alias("g"), F.col("g.team_group") == F.col("s.team_group"), "inner")
    .filter(F.col("s.level") > F.col("g.avg_level"))
    .select(
        "s.*",
        "g.avg_level",
        F.round(F.col("s.level") - F.col("g.avg_level"), 2).alias("level_minus_avg"),
    )
)

grouped = (
    compared
    .filter(F.col("team_group") == "group_a")
    .groupBy("team", "team_group", "level")
    .agg(
        F.count(F.when(F.col("team").isNotNull(), 1)).alias("n_team_is_not_null"),
        F.count("*").alias("row_count"),
        F.max("first_claim_reported_at").alias("last_first_claim_reported_at"),
    )
)

filtered = (
    grouped
    .crossJoin(grouped.agg(F.avg("row_count").alias("overall_avg_row_count")))
    .filter(F.col("row_count") > F.col("overall_avg_row_count"))
    .drop("overall_avg_row_count")
)

with_activity = (
    filtered
    .crossJoin(filtered.agg(F.avg("level").alias("overall_avg_level")))
    .filter(F.col("level") > F.col("overall_avg_level"))
    .drop("overall_avg_level")
)

labeled = (
    with_activity
    .filter(F.col("team_group") != "group_b")
    .select(
        "*",
        F.concat_ws("|", F.col("team_group"), F.col("team")).alias("team_group_team_label"),
        F.substring(F.trim(F.col("team_group")), 1, 4).alias("team_group_prefix"),
    )
)

result = (
    labeled
    .select(
        "team",
        "level",
        "n_team_is_not_null",
        "row_count",
        "last_first_claim_reported_at",
        "team_group_team_label",
        "team_group_prefix",
        F.when(F.col("team_group").isin("group_a", "other"), "group_a").when(F.col("team_group") == "group_b", "group_b").otherwise("other").alias("team_group_group"),
        F.least(F.col("row_count"), F.col("n_team_is_not_null")).alias("least_row_count_n_team_is_not_null"),
    )
    .orderBy(F.col("row_count").desc_nulls_first())
)
