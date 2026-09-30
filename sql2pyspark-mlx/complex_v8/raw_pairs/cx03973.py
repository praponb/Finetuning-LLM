from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_team = Window.partitionBy("team")
w_team_group = Window.partitionBy("team_group")
w_level_team_group_by_level = Window.partitionBy("level", "team_group").orderBy(F.col("level").desc(), "id")
w_level_team_group_by_level_ru = Window.partitionBy("level", "team_group").orderBy(F.col("level").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_level_team_by_level = Window.partitionBy("level", "team").orderBy(F.col("level").asc_nulls_last(), "id")
w_team_by_level = Window.partitionBy("team").orderBy(F.col("level").desc(), "id")
w_team_by_level_moving_6 = Window.partitionBy("team").orderBy(F.col("level").desc(), "id").rowsBetween(-6, Window.currentRow)
w_team_by_level_moving_1 = Window.partitionBy("team").orderBy(F.col("level").desc(), "id").rowsBetween(-1, Window.currentRow)
w_all_by_rows_in_team_moving_3 = Window.orderBy(F.col("rows_in_team").desc(), "id").rowsBetween(-3, Window.currentRow)

base_adjusters = (
    df_adjusters
    .filter(F.col("team") == "Motor")
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.when(F.col("team").isin("Property", "Health"), "group_a").when(F.col("team") == "Motor", "group_b").otherwise("other").alias("team_group"),
    )
)

windowed = (
    base_adjusters
    .select(
        "*",
        F.size(F.collect_set("team_group").over(w_team)).alias("distinct_team_group_in_team"),
        F.round(F.avg("level").over(w_team_group), 2).alias("team_group_avg_level"),
        F.lead("level", 1, 0).over(w_level_team_group_by_level).alias("next_level"),
        F.sum("level").over(w_level_team_group_by_level_ru).alias("running_level"),
        F.round(F.col("level") / F.nullif(F.sum("level").over(w_team), F.lit(0)), 4).alias("level_share_of_team"),
        F.count(F.lit(1)).over(w_team).alias("rows_in_team"),
        F.size(F.collect_set("team").over(w_team_group)).alias("distinct_team_in_team_group"),
        F.row_number().over(w_level_team_by_level).alias("seq_no"),
        F.dense_rank().over(w_team_by_level).alias("level_dense_rank"),
        F.round(F.avg("level").over(w_team_by_level_moving_6), 2).alias("level_avg_7"),
        (F.col("level") - F.lag("level").over(w_team_by_level)).alias("level_change"),
        F.sum("level").over(w_team_by_level_moving_1).alias("level_sum_2"),
        F.ntile(4).over(w_team_by_level).alias("level_quartile"),
    )
)

annotated = (
    windowed
    .filter(F.col("adjuster_name").isNotNull())
    .withColumn("rows_in_team_tier", F.when(F.col("rows_in_team") >= 12, "A").when(F.col("rows_in_team") >= 7, "B").otherwise("C"))
)

with_windows = (
    annotated
    .select(
        "*",
        F.round(F.col("next_level") / F.nullif(F.sum("next_level").over(w_team_group), F.lit(0)), 4).alias("next_level_share_of_team_group"),
        F.sum("level_change").over(w_team_group).alias("team_group_sum_level_change"),
        F.sum("distinct_team_group_in_team").over(w_all_by_rows_in_team_moving_3).alias("distinct_team_group_in_team_sum_4"),
        F.count(F.lit(1)).over(w_team_group).alias("rows_in_team_group"),
    )
)

result = (
    with_windows
    .filter(F.col("team_group_avg_level") >= 1.6)
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        "distinct_team_group_in_team",
        "team_group_avg_level",
        "next_level",
        "running_level",
        "rows_in_team",
        "distinct_team_in_team_group",
        "level_dense_rank",
        "level_sum_2",
        "rows_in_team_tier",
        "next_level_share_of_team_group",
        "team_group_sum_level_change",
        "rows_in_team_group",
        F.round(F.col("level_change") / F.nullif(F.col("level_dense_rank"), F.lit(0)), 3).alias("level_change_per_level_dense_rank"),
        F.lower(F.regexp_replace(F.trim(F.col("team_group")), " ", "_")).alias("team_group_slug"),
    )
    .orderBy(F.col("level_dense_rank").desc())
)
