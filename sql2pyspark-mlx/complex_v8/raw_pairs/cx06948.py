from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_team_level = Window.partitionBy("team", "level")
w_all_by_level_ties = Window.orderBy(F.col("level").desc())
w_team_level_by_level = Window.partitionBy("team", "level").orderBy(F.col("level").desc(), "id")
w_all_by_level_running = Window.orderBy(F.col("level").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_level_team_by_level_full = Window.partitionBy("level", "team").orderBy(F.col("level").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_level_by_level = Window.partitionBy("level").orderBy(F.col("level").desc(), "id")
w_level_by_level_ties = Window.partitionBy("level").orderBy(F.col("level").desc())
w_team = Window.partitionBy("team")
w_level_by_max_level_so_far_ru = Window.partitionBy("level").orderBy("max_level_so_far", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_second_level_level_by_level = Window.partitionBy("second_level", "level").orderBy("level_rank", "id").rowsBetween(-6, Window.currentRow)
w_all_by_greatest_min_level_so = Window.orderBy(F.col("greatest_min_level_so_far_level_dense_rank").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_level_rank_second_level_by_r = Window.partitionBy("level_rank", "second_level").orderBy("rn")

scoped_adjusters = (
    df_adjusters
    .filter(F.col("adjuster_name").isNotNull())
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.substring(F.trim(F.col("adjuster_name")), 1, 4).alias("adjuster_name_prefix"),
    )
)

with_trends = (
    scoped_adjusters
    .select(
        "*",
        F.rank().over(w_all_by_level_ties).alias("level_rank"),
        (F.col("level") - F.lag("level").over(w_team_level_by_level)).alias("level_change"),
        F.min("level").over(w_all_by_level_running).alias("min_level_so_far"),
        F.max("level").over(w_all_by_level_running).alias("max_level_so_far"),
        F.nth_value("level", 2).over(w_level_team_by_level_full).alias("second_level"),
        F.round(F.cume_dist().over(w_level_by_level), 3).alias("level_cume_dist"),
        F.dense_rank().over(w_level_by_level_ties).alias("level_dense_rank"),
        F.round(F.col("level") - F.avg("level").over(w_team_level), 2).alias("level_vs_team_level_avg"),
        F.row_number().over(w_level_by_level).alias("rn"),
    )
)

annotated = (
    with_trends
    .filter(F.col("second_level") < 3)
    .withColumn("greatest_min_level_so_far_level_dense_rank", F.greatest(F.col("min_level_so_far"), F.col("level_dense_rank")))
)

with_windows = (
    annotated
    .select(
        "*",
        F.count(F.lit(1)).over(w_team).alias("rows_in_team"),
        F.first("max_level_so_far", ignorenulls=True).over(w_level_by_max_level_so_far_ru).alias("first_known_max_level_so_far"),
        F.round(F.avg("max_level_so_far").over(w_second_level_level_by_level), 2).alias("max_level_so_far_avg_7"),
        F.max("level_dense_rank").over(w_team).alias("team_max_level_dense_rank"),
        F.first("second_level").over(w_all_by_greatest_min_level_so).alias("first_second_level"),
        F.round(F.cume_dist().over(w_level_rank_second_level_by_r), 3).alias("rn_cume_dist"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        "adjuster_name_prefix",
        "level_change",
        "min_level_so_far",
        "max_level_so_far",
        "second_level",
        "level_dense_rank",
        "level_vs_team_level_avg",
        "rn",
        "greatest_min_level_so_far_level_dense_rank",
        "first_known_max_level_so_far",
        "max_level_so_far_avg_7",
        "team_max_level_dense_rank",
        "first_second_level",
        "rn_cume_dist",
        F.concat_ws("|", F.col("team"), F.col("adjuster_name_prefix")).alias("team_adjuster_name_prefix_label"),
        F.when(F.col("level_change") >= 2, "A").when(F.col("level_change") >= -2, "B").otherwise("C").alias("level_change_tier"),
    )
    .orderBy("level_dense_rank", F.col("adjuster_name").desc_nulls_first())
)
