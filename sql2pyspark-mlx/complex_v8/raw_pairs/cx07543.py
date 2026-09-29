from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_length_trim_adjuster_name = Window.partitionBy("n_length_trim_adjuster_name_7").orderBy(F.col("level_filled_team_not_in_property_travel").asc_nulls_last(), "team").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_n_length_trim_adjuster_name_2 = Window.partitionBy("n_length_trim_adjuster_name_7").orderBy(F.col("level_filled_team_not_in_property_travel").asc_nulls_last(), "team").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_n_length_trim_adjuste = Window.orderBy("n_length_trim_adjuster_name_7", "team").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_n_length_trim_adjuster_name_3 = Window.partitionBy("n_length_trim_adjuster_name_8").orderBy(F.col("n_length_trim_adjuster_name_7").asc_nulls_last(), "team")
w_min_level = Window.partitionBy("min_level")
w_team_min_level_by_min_level = Window.partitionBy("team", "min_level").orderBy("min_level")
w_team_min_level_by_min_level_2 = Window.partitionBy("team", "min_level").orderBy("min_level", "team")

filtered_adjusters = (
    df_adjusters
    .filter(F.lower(F.col("adjuster_name")).like("%fa%"))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.concat(F.col("team"), F.lit("#"), F.col("id").cast("string")).alias("team_ref"),
        F.when(F.col("level") >= 3, "A").when(F.col("level") >= 2, "B").otherwise("C").alias("level_tier"),
        F.coalesce(F.col("level"), F.lit(0)).alias("level_filled"),
    )
)

per_group = (
    filtered_adjusters
    .groupBy("team")
    .agg(
        F.sum(F.when(~F.col("team").isin("Property", "Travel"), F.col("level_filled"))).alias("level_filled_team_not_in_property_travel"),
        F.count(F.when(F.length(F.trim(F.col("adjuster_name"))) >= 7, 1)).alias("n_length_trim_adjuster_name_7"),
        F.min("level").alias("min_level"),
        F.count(F.when(F.length(F.trim(F.col("adjuster_name"))) >= 8, 1)).alias("n_length_trim_adjuster_name_8"),
        F.array_sort(F.collect_set("adjuster_name")).alias("adjuster_name_list"),
    )
)

metrics = (
    per_group
    .select(
        "*",
        F.first("min_level").over(w_n_length_trim_adjuster_name).alias("first_min_level"),
        F.count(F.lit(1)).over(w_n_length_trim_adjuster_name_2).alias("running_count"),
        F.nth_value("min_level", 2).over(w_all_by_n_length_trim_adjuste).alias("second_min_level"),
        (F.col("min_level") - F.lag("min_level").over(w_n_length_trim_adjuster_name_3)).alias("min_level_change"),
    )
)

metrics_2 = (
    metrics
    .select(
        "team",
        "level_filled_team_not_in_property_travel",
        "n_length_trim_adjuster_name_7",
        "min_level",
        "adjuster_name_list",
        "running_count",
        "second_min_level",
        "min_level_change",
        F.round(F.avg("min_level").over(w_min_level), 2).alias("min_level_avg_min_level"),
        F.round(F.percent_rank().over(w_team_min_level_by_min_level), 3).alias("min_level_percent_rank"),
        F.size(F.collect_set("team").over(w_min_level)).alias("distinct_team_in_min_level"),
        F.round(F.cume_dist().over(w_team_min_level_by_min_level_2), 3).alias("min_level_cume_dist"),
        F.max("first_min_level").over(w_min_level).alias("min_level_max_first_min_level"),
    )
)

result = (
    metrics_2
    .groupBy("min_level_change", "n_length_trim_adjuster_name_7")
    .pivot("team", ["Health", "Property", "Travel", "Motor"])
    .agg(F.avg("running_count"))
    .orderBy("min_level_change", "n_length_trim_adjuster_name_7")
)
