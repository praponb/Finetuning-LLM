from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_level_filled_level = Window.partitionBy("level_filled", "level")
w_team_level_filled_by_level_f = Window.partitionBy("team", "level_filled").orderBy(F.col("level_filled").desc())
w_team_level_filled_by_level_f_2 = Window.partitionBy("team", "level_filled").orderBy(F.col("level_filled").desc(), "id")

filtered = (
    df_adjusters
    .filter((F.length(F.trim(F.col("adjuster_name"))) >= 7) & (F.col("level") <= 3))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.coalesce(F.col("level"), F.lit(0)).alias("level_filled"),
    )
)

windowed = (
    filtered
    .select(
        "*",
        F.size(F.collect_set("team").over(w_level_filled_level)).alias("distinct_team_in_level_filled_level"),
        F.dense_rank().over(w_team_level_filled_by_level_f).alias("level_filled_dense_rank"),
        F.sum("level_filled").over(w_level_filled_level).alias("level_filled_level_sum_level_filled"),
        F.max("level_filled").over(w_level_filled_level).alias("level_filled_level_max_level_filled"),
        F.lead("level").over(w_team_level_filled_by_level_f_2).alias("next_level"),
        F.round(F.col("level_filled") - F.avg("level_filled").over(w_level_filled_level), 2).alias("level_filled_vs_level_filled_level_avg"),
        F.ntile(10).over(w_team_level_filled_by_level_f_2).alias("level_filled_decile"),
        F.count(F.lit(1)).over(w_level_filled_level).alias("rows_in_level_filled_level"),
        (F.col("level_filled") - F.lag("level_filled").over(w_team_level_filled_by_level_f_2)).alias("level_filled_change"),
    )
)

leaders = (
    windowed
    .filter((F.col("level_filled_dense_rank") == 1) & (F.col("level_filled_change") <= 2))
    .drop("level_filled_dense_rank")
)

result = (
    leaders
    .select(
        "id",
        "level",
        "level_filled",
        "distinct_team_in_level_filled_level",
        "level_filled_level_sum_level_filled",
        "level_filled_level_max_level_filled",
        "next_level",
        "level_filled_vs_level_filled_level_avg",
        "level_filled_decile",
        "level_filled_change",
    )
    .orderBy(F.col("id").desc())
)
