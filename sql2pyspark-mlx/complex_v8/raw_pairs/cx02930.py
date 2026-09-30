from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_level_team_by_level = Window.partitionBy("level", "team").orderBy("level", "id")
w_level_by_level = Window.partitionBy("level").orderBy("level", "id")
w_level_team_by_level_ties = Window.partitionBy("level", "team").orderBy("level")
w_level_by_level_running = Window.partitionBy("level").orderBy("level", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_adjusters = (
    df_adjusters
    .filter((F.col("team") == "Travel") & (F.col("level") > 2))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
    )
)

with_windows = (
    prepared_adjusters
    .select(
        "*",
        F.lag("level", 1, 0).over(w_level_team_by_level).alias("prev_level"),
        F.ntile(10).over(w_level_by_level).alias("level_decile"),
        F.round(F.cume_dist().over(w_level_by_level), 3).alias("level_cume_dist"),
        F.round(F.percent_rank().over(w_level_team_by_level_ties), 3).alias("level_percent_rank"),
        F.first("level", ignorenulls=True).over(w_level_by_level_running).alias("first_known_level"),
    )
)

result = (
    with_windows
    .cube("level", "level_decile")
    .agg(
        F.array_sort(F.collect_set("adjuster_name")).alias("adjuster_name_list"),
    )
    .orderBy(F.col("level").asc_nulls_last(), F.col("level_decile").asc_nulls_last())
)
