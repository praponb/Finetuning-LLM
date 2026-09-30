from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_level_team = Window.partitionBy("level", "team")
w_team_level_by_level = Window.partitionBy("team", "level").orderBy("level", "id")
w_level_by_level = Window.partitionBy("level").orderBy("level", "id")
w_team_level_by_level_moving_6 = Window.partitionBy("team", "level").orderBy(F.col("level").desc_nulls_first(), "id").rowsBetween(-6, Window.currentRow)
w_team_level_by_level_running = Window.partitionBy("team", "level").orderBy("level", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_adjusters
    .filter(F.col("level") < 3)
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.concat_ws("-", F.col("adjuster_name"), F.col("team")).alias("adjuster_name_team_label"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.round(F.col("level") - F.avg("level").over(w_level_team), 2).alias("level_vs_level_team_avg"),
        F.row_number().over(w_team_level_by_level).alias("seq_no"),
        F.ntile(10).over(w_level_by_level).alias("level_decile"),
        F.min("level").over(w_level_team).alias("level_team_min_level"),
        F.round(F.avg("level").over(w_team_level_by_level_moving_6), 2).alias("level_avg_7"),
        F.first("level", ignorenulls=True).over(w_team_level_by_level_running).alias("first_known_level"),
    )
)

team_stats = (
    with_windows
    .groupBy("team")
    .agg(F.avg("level_team_min_level").alias("avg_level_team_min_level"))
)

outliers = (
    with_windows.alias("s")
    .join(team_stats.alias("g"), F.col("g.team") == F.col("s.team"), "inner")
    .filter(F.col("s.level_team_min_level") > F.col("g.avg_level_team_min_level"))
    .select(
        "s.*",
        "g.avg_level_team_min_level",
        F.round(F.col("s.level_team_min_level") - F.col("g.avg_level_team_min_level"), 2).alias("level_team_min_level_minus_avg"),
    )
)

per_group = (
    outliers
    .groupBy(
        F.when(F.col("avg_level_team_min_level") >= 30.4, "HIGH").when(F.col("avg_level_team_min_level") >= 13.2, "MEDIUM").otherwise("LOW").alias("avg_level_team_min_level_tier"),
        "level",
        "first_known_level",
    )
    .agg(
        F.max("first_known_level").alias("max_first_known_level"),
        F.array_sort(F.collect_set("adjuster_name")).alias("adjuster_name_list"),
        F.sum(F.when(F.col("adjuster_name").isNotNull(), F.col("level_team_min_level")).otherwise(0)).alias("level_team_min_level_adjuster_name_is_not_null"),
        F.round(F.avg("level"), 2).alias("avg_level"),
    )
)

result = (
    per_group
    .select(
        "avg_level_team_min_level_tier",
        "level",
        "first_known_level",
        "max_first_known_level",
    )
    .unpivot(
        ids=["avg_level_team_min_level_tier", "level"],
        values=["first_known_level", "max_first_known_level"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("avg_level_team_min_level_tier", "level", "measure")
)
