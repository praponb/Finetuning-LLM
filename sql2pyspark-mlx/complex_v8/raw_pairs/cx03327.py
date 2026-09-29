from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_team_by_avg_level_ties = Window.partitionBy("team").orderBy(F.col("avg_level").desc())
w_team_by_avg_level_full = Window.partitionBy("team").orderBy(F.col("avg_level").desc(), "team", "level").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_team_by_avg_level = Window.partitionBy("team").orderBy(F.col("avg_level").desc(), "team", "level")
w_level = Window.partitionBy("level")

filtered_adjusters = (
    df_adjusters
    .filter((F.upper(F.trim(F.col("adjuster_name"))).like("A%")) | (F.col("team") == "Travel"))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
    )
)

summary = (
    filtered_adjusters
    .filter(F.col("level") <= 3)
    .groupBy("team", "level")
    .agg(
        F.count(F.when(F.length(F.trim(F.col("adjuster_name"))) >= 8, 1)).alias("n_length_trim_adjuster_name_8"),
        F.array_sort(F.collect_set("adjuster_name")).alias("adjuster_name_list"),
        F.round(F.avg("level"), 2).alias("avg_level"),
    )
)

screened = (
    summary
    .crossJoin(summary.agg(F.max("level").alias("overall_max_level")))
    .filter(F.col("level") < F.col("overall_max_level"))
    .drop("overall_max_level")
)

ranked = (
    screened
    .select(
        "*",
        F.round(F.percent_rank().over(w_team_by_avg_level_ties), 3).alias("avg_level_percent_rank"),
        F.nth_value("level", 2).over(w_team_by_avg_level_full).alias("second_level"),
        F.lag("avg_level", 2).over(w_team_by_avg_level).alias("prev2_avg_level"),
        F.round(F.col("level") / F.nullif(F.sum("level").over(w_level), F.lit(0)), 4).alias("level_share_of_level"),
        F.lag("level").over(w_team_by_avg_level).alias("prev_level"),
    )
)

result = (
    ranked
    .filter(F.col("second_level") >= 1)
    .select(
        "team",
        "level",
        "n_length_trim_adjuster_name_8",
        "avg_level_percent_rank",
        "second_level",
        "prev2_avg_level",
        "level_share_of_level",
        "prev_level",
    )
    .orderBy("n_length_trim_adjuster_name_8", "prev2_avg_level")
)
