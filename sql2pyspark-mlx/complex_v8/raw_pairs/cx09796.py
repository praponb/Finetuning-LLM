from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_team_by_level_full = Window.partitionBy("team").orderBy(F.col("level").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_team = Window.partitionBy("team")
w_team_by_level_ties = Window.partitionBy("team").orderBy(F.col("level").asc_nulls_last())
w_team_by_level_running = Window.partitionBy("team").orderBy(F.col("level").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_team_len_claim_count_by_leve = Window.partitionBy("team_len", "claim_count").orderBy(F.col("level_rank").desc())
w_level = Window.partitionBy("level")
w_team_len_claim_count_by_leve_2 = Window.partitionBy("team_len", "claim_count").orderBy(F.col("level_rank").desc(), "id")

valid = (
    df_adjusters
    .filter((F.col("adjuster_name").isNotNull()) & (F.col("team").isNotNull()))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.upper(F.trim(F.col("adjuster_name"))).alias("adjuster_name_clean"),
        F.length(F.trim(F.col("team"))).alias("team_len"),
    )
)

sequenced = (
    valid
    .select(
        "*",
        F.nth_value("level", 2).over(w_team_by_level_full).alias("second_level"),
        F.count(F.lit(1)).over(w_team).alias("rows_in_team"),
        F.rank().over(w_team_by_level_ties).alias("level_rank"),
        F.min("level").over(w_team_by_level_running).alias("min_level_so_far"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(F.count("*").alias("claim_count"))
)

summarized_rows = (
    sequenced.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

labeled = (
    summarized_rows
    .select(
        "*",
        (F.col("min_level_so_far") + F.col("rows_in_team")).alias("min_level_so_far_plus_rows_in_team"),
        F.concat(F.col("adjuster_name"), F.lit("#"), F.col("rows_in_team").cast("string")).alias("adjuster_name_ref"),
    )
)

team_stats = (
    labeled
    .groupBy("team")
    .agg(F.percentile_approx("second_level", 0.5).alias("median_second_level"))
)

vs_group = (
    labeled.alias("s")
    .join(team_stats.alias("g"), F.col("g.team") == F.col("s.team"), "inner")
    .filter(F.col("s.second_level") >= F.col("g.median_second_level"))
    .select(
        "s.*",
        "g.median_second_level",
        F.round(F.col("s.second_level") - F.col("g.median_second_level"), 2).alias("second_level_minus_median"),
    )
)

with_trends = (
    vs_group
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        "adjuster_name_clean",
        "team_len",
        "second_level",
        "min_level_so_far",
        "claim_count",
        "adjuster_name_ref",
        "median_second_level",
        "second_level_minus_median",
        F.round(F.cume_dist().over(w_team_len_claim_count_by_leve), 3).alias("level_rank_cume_dist"),
        F.max("team_len").over(w_level).alias("level_max_team_len"),
        F.lag("rows_in_team", 1, 0).over(w_team_len_claim_count_by_leve_2).alias("prev_rows_in_team"),
        F.row_number().over(w_team_len_claim_count_by_leve_2).alias("rn"),
        F.max("second_level").over(w_level).alias("level_max_second_level"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        "adjuster_name_clean",
        "second_level",
        "min_level_so_far",
        "claim_count",
        "adjuster_name_ref",
        "median_second_level",
        "second_level_minus_median",
        "level_rank_cume_dist",
        "level_max_team_len",
        "prev_rows_in_team",
        "level_max_second_level",
        F.concat(F.col("adjuster_name_ref"), F.lit("#"), F.col("prev_rows_in_team").cast("string")).alias("adjuster_name_ref_ref"),
    )
    .orderBy(F.col("median_second_level").desc(), F.col("prev_rows_in_team").asc())
)
