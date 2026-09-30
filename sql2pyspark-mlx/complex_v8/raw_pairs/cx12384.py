from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_level_median_level = Window.partitionBy("level", "median_level")

valid = (
    df_adjusters
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
    )
)

totals = (
    valid
    .filter(F.col("team").isin("Property", "Travel", "Health"))
    .groupBy("level", "team")
    .agg(
        F.array_sort(F.collect_set("adjuster_name")).alias("adjuster_name_list"),
        F.array_sort(F.collect_set("team")).alias("team_list"),
        F.percentile_approx("level", 0.5).alias("median_level"),
        F.max("level").alias("max_level"),
        F.avg("level").alias("avg_level_check"),
    )
    .filter(F.col("avg_level_check") > 2)
    .drop("avg_level_check")
)

scored = (
    totals
    .filter(F.col("max_level") <= 3)
    .withColumn("team_group", F.when(F.col("team").isin("Property", "Health"), "group_a").when(F.col("team") == "Travel", "group_b").otherwise("other"))
)

screened = (
    scored
    .crossJoin(scored.agg(F.avg("max_level").alias("overall_avg_max_level")))
    .filter(F.col("max_level") > F.col("overall_avg_max_level"))
    .drop("overall_avg_max_level")
)

metrics = (
    screened
    .select(
        "level",
        "team",
        "median_level",
        "max_level",
        F.count(F.lit(1)).over(w_level_median_level).alias("rows_in_level_median_level"),
        F.max("level").over(w_level_median_level).alias("level_median_level_max_level"),
        F.size(F.collect_set("team_group").over(w_level_median_level)).alias("distinct_team_group_in_level_median_level"),
    )
)

result = (
    metrics
    .filter(F.col("rows_in_level_median_level") > 7)
    .select(
        "level",
        "team",
        "median_level",
        "max_level",
        "distinct_team_group_in_level_median_level",
        F.abs(F.col("max_level") - 3).alias("max_level_distance"),
        F.coalesce(F.col("level"), F.lit(0)).alias("level_filled"),
    )
    .orderBy(
        F.col("median_level").asc_nulls_last(),
        F.col("distinct_team_group_in_level_median_level").desc(),
    )
)
