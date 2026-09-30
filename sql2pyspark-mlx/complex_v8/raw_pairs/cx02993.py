from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_team_level_filled_by_level = Window.partitionBy("team", "level_filled").orderBy("level", "id")
w_level_filled = Window.partitionBy("level_filled")
w_level_filled_by_team_len_tie = Window.partitionBy("level_filled").orderBy("team_len")
w_team_level_filled_by_level_t = Window.partitionBy("team", "level_filled").orderBy("level")

recent_adjusters = (
    df_adjusters
    .filter((~F.col("team").isin("Property", "Motor")) & (F.upper(F.trim(F.col("adjuster_name"))).like("H%")))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.length(F.trim(F.col("team"))).alias("team_len"),
        F.coalesce(F.col("level"), F.lit(0)).alias("level_filled"),
        F.when(F.col("team").isin("Motor", "Health"), "group_a").when(F.col("team") == "Property", "group_b").otherwise("other").alias("team_group"),
    )
)

windowed = (
    recent_adjusters
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        "team_len",
        "level_filled",
        F.row_number().over(w_team_level_filled_by_level).alias("rn"),
        F.size(F.collect_set("team").over(w_level_filled)).alias("distinct_team_in_level_filled"),
        F.dense_rank().over(w_level_filled_by_team_len_tie).alias("team_len_dense_rank"),
        F.dense_rank().over(w_team_level_filled_by_level_t).alias("level_dense_rank"),
        F.count(F.lit(1)).over(w_level_filled).alias("rows_in_level_filled"),
        F.round(F.col("team_len") - F.avg("team_len").over(w_level_filled), 2).alias("team_len_vs_level_filled_avg"),
    )
)

result = (
    windowed
    .cube("rn", "team_len", "team_len_dense_rank")
    .agg(
        F.count(F.when(F.col("team").isin("Motor", "Property"), 1)).alias("n_team_in_motor_property"),
        F.count("*").alias("n_rows"),
    )
    .orderBy(
        F.col("rn").asc_nulls_last(),
        F.col("team_len").asc_nulls_last(),
        F.col("team_len_dense_rank").asc_nulls_last(),
    )
)
