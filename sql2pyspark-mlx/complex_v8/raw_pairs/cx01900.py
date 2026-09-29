from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_level_distance_level_by_leve = Window.partitionBy("level_distance", "level").orderBy("level_distance_minus_avg")
w_level_distance_level_by_leve_2 = Window.partitionBy("level_distance", "level").orderBy("level_distance_minus_avg", "id")

valid_adjusters = (
    df_adjusters
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.abs(F.col("level") - 2).alias("level_distance"),
    )
)

team_stats = (
    valid_adjusters
    .groupBy("team")
    .agg(F.avg("level_distance").alias("avg_level_distance"))
)

outliers = (
    valid_adjusters.alias("s")
    .join(team_stats.alias("g"), F.col("g.team") == F.col("s.team"), "inner")
    .filter(F.col("s.level_distance") > F.col("g.avg_level_distance"))
    .select(
        "s.*",
        "g.avg_level_distance",
        F.round(F.col("s.level_distance") - F.col("g.avg_level_distance"), 2).alias("level_distance_minus_avg"),
    )
)

windowed = (
    outliers
    .select(
        "*",
        F.round(F.percent_rank().over(w_level_distance_level_by_leve), 3).alias("level_distance_minus_avg_percent_rank"),
        F.lag("level_distance_minus_avg", 3).over(w_level_distance_level_by_leve_2).alias("prev3_level_distance_minus_avg"),
        F.rank().over(w_level_distance_level_by_leve_2).alias("level_distance_minus_avg_rank"),
    )
)

result = (
    windowed
    .select(
        "id",
        "prev3_level_distance_minus_avg",
        "level_distance_minus_avg",
        "level_distance_minus_avg_percent_rank",
    )
    .unpivot(
        ids=["id"],
        values=["prev3_level_distance_minus_avg", "level_distance_minus_avg", "level_distance_minus_avg_percent_rank"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
