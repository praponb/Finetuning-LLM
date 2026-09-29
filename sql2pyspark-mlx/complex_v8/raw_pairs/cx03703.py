from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_level = Window.orderBy(F.col("level").desc(), "id")
w_level = Window.partitionBy("level")
w_team_level = Window.partitionBy("team", "level")
w_all_by_level_ties = Window.orderBy(F.col("level").desc())
w_level_team_by_level_full = Window.partitionBy("level", "team").orderBy("level", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_team_by_level_ties = Window.partitionBy("team").orderBy(F.col("level").desc())

filtered = (
    df_adjusters
    .filter((F.length(F.trim(F.col("adjuster_name"))) >= 4) | (F.col("level") >= 1))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
    )
)

ranked = (
    filtered
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.row_number().over(w_all_by_level).alias("level_seq"),
        F.count(F.lit(1)).over(w_level).alias("rows_in_level"),
        F.min("level").over(w_level).alias("level_min_level"),
        F.round(F.col("level") - F.avg("level").over(w_level), 2).alias("level_vs_level_avg"),
        F.sum("level").over(w_level).alias("level_sum_level"),
        F.round(F.percent_rank().over(w_all_by_level_ties), 3).alias("level_percent_rank"),
        F.lag("level", 3, 0).over(w_all_by_level).alias("prev3_level"),
        F.first("level").over(w_level_team_by_level_full).alias("first_level"),
        F.round(F.cume_dist().over(w_team_by_level_ties), 3).alias("level_cume_dist"),
    )
)

top_rows = (
    ranked
    .filter(F.col("level_seq") <= 3)
    .drop("level_seq")
)

result = (
    top_rows
    .select(
        "id",
        "team",
        "rows_in_level",
        "level_min_level",
        "level_vs_level_avg",
        "level_percent_rank",
        "prev3_level",
        "first_level",
        F.length(F.trim(F.col("team"))).alias("team_len"),
        F.concat_ws(": ", F.col("adjuster_name"), F.col("team")).alias("adjuster_name_team_label"),
    )
    .orderBy("first_level")
)
