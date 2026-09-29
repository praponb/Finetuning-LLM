from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_level_team_by_level_ties = Window.partitionBy("level", "team").orderBy("level")
w_level_team_by_level = Window.partitionBy("level", "team").orderBy(F.col("level").desc(), "id")
w_all_by_avg_level_full = Window.orderBy("avg_level", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_level_team_by_level_full = Window.partitionBy("level", "team").orderBy(F.col("level").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_level_decile_level_by_level = Window.partitionBy("level_decile", "level").orderBy(F.col("level_minus_avg").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_level_decile_level_by_level_2 = Window.partitionBy("level_decile", "level").orderBy(F.col("level_minus_avg").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claim_count_second_level = Window.partitionBy("claim_count", "second_level")
w_level_decile_level_by_level_3 = Window.partitionBy("level_decile", "level").orderBy(F.col("level_minus_avg").desc_nulls_first(), "id")
w_dedup = Window.partitionBy("team").orderBy("level_minus_avg", "id")

recent_adjusters = (
    df_adjusters
    .filter(F.col("adjuster_name").isNotNull())
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.concat_ws("-", F.col("adjuster_name"), F.col("team")).alias("adjuster_name_team_label"),
    )
)

team_stats = (
    recent_adjusters
    .groupBy("team")
    .agg(F.avg("level").alias("avg_level"))
)

above_average = (
    recent_adjusters.alias("s")
    .join(team_stats.alias("g"), F.col("g.team") == F.col("s.team"), "inner")
    .filter(F.col("s.level") > F.col("g.avg_level"))
    .select(
        "s.*",
        "g.avg_level",
        F.round(F.col("s.level") - F.col("g.avg_level"), 2).alias("level_minus_avg"),
    )
)

with_trends = (
    above_average
    .select(
        "*",
        F.round(F.cume_dist().over(w_level_team_by_level_ties), 3).alias("level_cume_dist"),
        F.ntile(10).over(w_level_team_by_level).alias("level_decile"),
        F.nth_value("avg_level", 2).over(w_all_by_avg_level_full).alias("second_avg_level"),
        F.nth_value("level", 2).over(w_level_team_by_level_full).alias("second_level"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(F.count("*").alias("claim_count"))
)

with_stats = (
    with_trends.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

windowed = (
    with_stats
    .select(
        "id",
        "adjuster_name",
        "team",
        "adjuster_name_team_label",
        "level_minus_avg",
        "level_decile",
        "second_avg_level",
        "second_level",
        "claim_count",
        F.sum("avg_level").over(w_level_decile_level_by_level).alias("running_avg_level"),
        F.nth_value("second_avg_level", 2).over(w_level_decile_level_by_level_2).alias("second_second_avg_level"),
        F.min("level_cume_dist").over(w_claim_count_second_level).alias("claim_count_second_level_min_level_cume_dist"),
        F.lag("second_avg_level").over(w_level_decile_level_by_level_3).alias("prev_second_avg_level"),
        F.sum("second_level").over(w_claim_count_second_level).alias("claim_count_second_level_sum_second_level"),
    )
)

latest = (
    windowed
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .select(
        "id",
        "adjuster_name",
        "adjuster_name_team_label",
        "level_minus_avg",
        "level_decile",
        "second_avg_level",
        "claim_count",
        "running_avg_level",
        "second_second_avg_level",
        "claim_count_second_level_min_level_cume_dist",
        "prev_second_avg_level",
        "claim_count_second_level_sum_second_level",
        F.substring(F.trim(F.col("adjuster_name")), 1, 2).alias("adjuster_name_prefix"),
    )
    .orderBy("id")
)
