from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_min_avg_level = Window.orderBy("min_avg_level", "level_distance", "team")
w_cnt_team = Window.partitionBy("cnt", "team")
w_distinct_teams_by_min_avg_le = Window.partitionBy("distinct_teams").orderBy(F.col("min_avg_level").asc_nulls_last(), "level_distance", "team").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_min_avg_level_full = Window.orderBy("min_avg_level", "level_distance", "team").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base_adjusters = (
    df_adjusters
    .filter((F.upper(F.trim(F.col("adjuster_name"))).like("G%")) & (F.col("level").between(1, 3)))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.lower(F.regexp_replace(F.trim(F.col("team")), " ", "_")).alias("team_slug"),
        F.abs(F.col("level") - 3).alias("level_distance"),
        F.lower(F.regexp_replace(F.trim(F.col("team")), " ", "_")).alias("team_slug_2"),
    )
)

team_stats = (
    base_adjusters
    .groupBy("team")
    .agg(F.avg("level").alias("avg_level"))
)

above_average = (
    base_adjusters.alias("s")
    .join(team_stats.alias("g"), F.col("g.team") == F.col("s.team"), "inner")
    .filter(F.col("s.level") > F.col("g.avg_level"))
    .select(
        "s.*",
        "g.avg_level",
        F.round(F.col("s.level") - F.col("g.avg_level"), 2).alias("level_minus_avg"),
    )
)

without_matches = (
    above_average.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.id")) & (F.col("c.status").isin("REJECTED", "APPROVED", "PAID")), "left_anti")
    .filter(F.col("avg_level").between(2.0, 4.6))
)

rollup_base = (
    without_matches
    .groupBy("level_distance", "team")
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum("level_minus_avg") / F.nullif(F.sum("level_distance"), F.lit(0)), 3).alias("level_minus_avg_to_level_distance_ratio"),
        F.min("avg_level").alias("min_avg_level"),
        F.countDistinct("team").alias("distinct_teams"),
    )
)

metrics = (
    rollup_base
    .select(
        "*",
        F.lead("cnt").over(w_all_by_min_avg_level).alias("next_cnt"),
        F.round(F.col("distinct_teams") / F.nullif(F.sum("distinct_teams").over(w_cnt_team), F.lit(0)), 4).alias("distinct_teams_share_of_cnt_team"),
        F.round(F.col("distinct_teams") - F.avg("distinct_teams").over(w_cnt_team), 2).alias("distinct_teams_vs_cnt_team_avg"),
        F.last("level_distance").over(w_distinct_teams_by_min_avg_le).alias("last_level_distance"),
        F.nth_value("min_avg_level", 2).over(w_all_by_min_avg_level_full).alias("second_min_avg_level"),
    )
)

result = (
    metrics
    .select(
        "level_distance",
        "team",
        "cnt",
        "level_minus_avg_to_level_distance_ratio",
        "distinct_teams",
        "next_cnt",
        "distinct_teams_share_of_cnt_team",
        "distinct_teams_vs_cnt_team_avg",
        "second_min_avg_level",
        F.concat(F.col("team"), F.lit("#"), F.col("level_distance").cast("string")).alias("team_ref"),
    )
    .orderBy(F.col("distinct_teams").desc())
)
