from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("team").orderBy(F.col("level").desc(), F.col("id").desc())

scoped = (
    df_adjusters
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.substring(F.trim(F.col("adjuster_name")), 1, 3).alias("adjuster_name_prefix"),
        F.substring(F.trim(F.col("adjuster_name")), 1, 3).alias("adjuster_name_prefix_2"),
    )
)

latest = (
    scoped
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

team_stats = (
    latest
    .groupBy("team")
    .agg(F.percentile_approx("level", 0.5).alias("median_level"))
)

outliers = (
    latest.alias("s")
    .join(team_stats.alias("g"), F.col("g.team") == F.col("s.team"), "inner")
    .filter(F.col("s.level") >= F.col("g.median_level"))
    .select(
        "s.*",
        "g.median_level",
        F.round(F.col("s.level") - F.col("g.median_level"), 2).alias("level_minus_median"),
    )
)

result = (
    outliers
    .cube("team", "level")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.array_sort(F.collect_set("team")).alias("team_list"),
    )
)
