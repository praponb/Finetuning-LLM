from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_level = Window.partitionBy("level")
w_all_by_level_full = Window.orderBy("level", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_level_ties = Window.orderBy("level")
w_all_by_level = Window.orderBy("level", "id")
w_level_by_level = Window.partitionBy("level").orderBy(F.col("level").asc_nulls_last(), "id")
w_all_by_level_2 = Window.orderBy(F.col("level").desc(), "id")
w_second_level_team = Window.partitionBy("second_level", "team")
w_all_by_level_vs_level_avg_ti = Window.orderBy("level_vs_level_avg")
w_all_by_level_vs_level_avg = Window.orderBy("level_vs_level_avg", "id")
w_team_prev_level_by_second_le = Window.partitionBy("team", "prev_level").orderBy(F.col("second_level").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_team_prev_level_by_second_le_2 = Window.partitionBy("team", "prev_level").orderBy(F.col("second_level").desc(), "id")

recent_adjusters = (
    df_adjusters
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
    )
)

windowed = (
    recent_adjusters
    .select(
        "id",
        "team",
        "level",
        F.round(F.col("level") - F.avg("level").over(w_level), 2).alias("level_vs_level_avg"),
        F.nth_value("level", 2).over(w_all_by_level_full).alias("second_level"),
        F.round(F.percent_rank().over(w_all_by_level_ties), 3).alias("level_percent_rank"),
        F.round(F.col("level") / F.nullif(F.sum("level").over(w_level), F.lit(0)), 4).alias("level_share_of_level"),
        F.ntile(4).over(w_all_by_level).alias("level_quartile"),
        (F.col("level") - F.lag("level").over(w_level_by_level)).alias("level_change"),
        F.lag("level", 1, 0).over(w_all_by_level_2).alias("prev_level"),
        F.count(F.lit(1)).over(w_level).alias("rows_in_level"),
        F.round(F.avg("level").over(w_level), 2).alias("level_avg_level"),
    )
)

with_trends = (
    windowed
    .select(
        "*",
        F.max("level_percent_rank").over(w_second_level_team).alias("second_level_team_max_level_percent_rank"),
        F.rank().over(w_all_by_level_vs_level_avg_ti).alias("level_vs_level_avg_rank"),
        F.dense_rank().over(w_all_by_level_vs_level_avg).alias("level_vs_level_avg_dense_rank"),
        F.round(F.col("prev_level") / F.nullif(F.sum("prev_level").over(w_second_level_team), F.lit(0)), 4).alias("prev_level_share_of_second_level_team"),
        F.first("level_avg_level", ignorenulls=True).over(w_team_prev_level_by_second_le).alias("first_known_level_avg_level"),
        F.round(F.col("rows_in_level") - F.avg("rows_in_level").over(w_second_level_team), 2).alias("rows_in_level_vs_second_level_team_avg"),
        F.round(F.col("second_level") / F.nullif(F.sum("second_level").over(w_second_level_team), F.lit(0)), 4).alias("second_level_share_of_second_level_team"),
        F.row_number().over(w_team_prev_level_by_second_le_2).alias("seq_no"),
    )
)

totals = (
    with_trends
    .groupBy("level_vs_level_avg_rank")
    .agg(
        F.sum(F.when(F.col("team") == "Motor", 1).otherwise(0)).alias("n_team_motor"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("team").isNotNull(), F.col("first_known_level_avg_level"))).alias("first_known_level_avg_level_team_is_not_null"),
    )
)

result = (
    totals
    .filter(F.col("level_vs_level_avg_rank") < 9)
    .select(
        "level_vs_level_avg_rank",
        "n_team_motor",
        "distinct_ids",
        "cnt",
        "first_known_level_avg_level_team_is_not_null",
        (F.floor(F.col("cnt") / 2) * 2).alias("cnt_bucket"),
        (F.floor(F.col("n_team_motor") / 2) * 2).alias("n_team_motor_bucket"),
    )
    .orderBy(F.col("cnt").asc_nulls_last())
)
