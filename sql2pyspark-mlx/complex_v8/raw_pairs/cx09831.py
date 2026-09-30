from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_team_by_level = Window.partitionBy("team").orderBy(F.col("level").desc(), "id")
w_team = Window.partitionBy("team")
w_level = Window.partitionBy("level")
w_level_team_by_level = Window.partitionBy("level", "team").orderBy("level", "id")
w_level_team_by_level_ties = Window.partitionBy("level", "team").orderBy("level")

scoped = (
    df_adjusters
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.upper(F.trim(F.col("adjuster_name"))).alias("adjuster_name_clean"),
    )
)

ranked = (
    scoped
    .select(
        "*",
        F.lag("level").over(w_team_by_level).alias("prev_level"),
        F.round(F.col("level") - F.avg("level").over(w_team), 2).alias("level_vs_team_avg"),
        F.round(F.avg("level").over(w_level), 2).alias("level_avg_level"),
        (F.col("level") - F.lag("level").over(w_level_team_by_level)).alias("level_change"),
        F.round(F.col("level") / F.nullif(F.sum("level").over(w_team), F.lit(0)), 4).alias("level_share_of_team"),
        F.dense_rank().over(w_level_team_by_level_ties).alias("level_dense_rank"),
        F.min("level").over(w_team).alias("team_min_level"),
        F.row_number().over(w_team_by_level).alias("seq_no"),
    )
)

top_rows = (
    ranked
    .filter(F.col("seq_no") == 1)
    .drop("seq_no")
)

result = (
    top_rows
    .select(
        "id",
        "level",
        "adjuster_name_clean",
        "prev_level",
        "level_vs_team_avg",
        "level_avg_level",
        "level_change",
        "level_share_of_team",
        "level_dense_rank",
        "team_min_level",
        F.length(F.trim(F.col("adjuster_name"))).alias("adjuster_name_len"),
    )
    .orderBy(F.col("adjuster_name_len").desc(), "level_share_of_team")
)
