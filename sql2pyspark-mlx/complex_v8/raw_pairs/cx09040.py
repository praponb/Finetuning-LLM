from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_min_level_full = Window.orderBy("min_level", "team_group", "level").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_max_level = Window.partitionBy("max_level")
w_all_by_min_level = Window.orderBy("min_level", "team_group", "level")

base_adjusters = (
    df_adjusters
    .filter(F.col("team").isin("Health", "Property", "Motor"))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.when(F.col("team").isin("Health", "Travel"), "group_a").when(F.col("team") == "Motor", "group_b").otherwise("other").alias("team_group"),
    )
)

totals = (
    base_adjusters
    .groupBy("team_group", "level")
    .agg(F.min("level").alias("min_level"), F.max("level").alias("max_level"))
)

windowed = (
    totals
    .select(
        "team_group",
        "level",
        "min_level",
        "max_level",
        F.nth_value("level", 2).over(w_all_by_min_level_full).alias("second_level"),
        F.sum("level").over(w_max_level).alias("max_level_sum_level"),
        F.lead("min_level").over(w_all_by_min_level).alias("next_min_level"),
        F.round(F.col("min_level") - F.avg("min_level").over(w_max_level), 2).alias("min_level_vs_max_level_avg"),
        F.nth_value("min_level", 2).over(w_all_by_min_level_full).alias("second_min_level"),
    )
)

qualified = (
    windowed
    .crossJoin(windowed.agg(F.min("max_level").alias("overall_min_max_level")))
    .filter(F.col("max_level") > F.col("overall_min_max_level"))
    .drop("overall_min_max_level")
)

result = (
    qualified
    .groupBy("second_level", "next_min_level")
    .pivot("team_group", ["group_b", "other", "group_a"])
    .agg(F.sum("min_level_vs_max_level_avg"))
)
