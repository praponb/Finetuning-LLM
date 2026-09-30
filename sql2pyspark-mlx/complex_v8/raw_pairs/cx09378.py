from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_team_level_by_level = Window.partitionBy("team", "level").orderBy("level", "id")
w_level = Window.partitionBy("level")
w_level_team = Window.partitionBy("level", "team")
w_team_level_by_level_full = Window.partitionBy("team", "level").orderBy(F.col("level").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_level = Window.orderBy("level", "id")
w_n_adjuster_name_is_not_null = Window.partitionBy("n_adjuster_name_is_not_null")
w_rows_in_level_tier_by_distin = Window.partitionBy("rows_in_level_tier").orderBy(F.col("distinct_teams").desc(), "rows_in_level_tier")
w_rows_in_level_tier_by_distin_2 = Window.partitionBy("rows_in_level_tier").orderBy(F.col("distinct_teams").desc(), "rows_in_level_tier").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid_adjusters = (
    df_adjusters
    .filter(F.col("adjuster_name").isNotNull())
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.lower(F.trim(F.col("adjuster_name"))).alias("adjuster_name_clean"),
        F.substring(F.trim(F.col("adjuster_name")), 1, 4).alias("adjuster_name_prefix"),
    )
)

metrics = (
    valid_adjusters
    .select(
        "id",
        "adjuster_name",
        "team",
        "adjuster_name_prefix",
        F.rank().over(w_team_level_by_level).alias("level_rank"),
        F.round(F.percent_rank().over(w_team_level_by_level), 3).alias("level_percent_rank"),
        F.round(F.col("level") / F.nullif(F.sum("level").over(w_level), F.lit(0)), 4).alias("level_share_of_level"),
        F.round(F.avg("level").over(w_level_team), 2).alias("level_team_avg_level"),
        F.nth_value("level", 2).over(w_team_level_by_level_full).alias("second_level"),
        F.row_number().over(w_all_by_level).alias("level_seq"),
        F.sum("level").over(w_level_team).alias("level_team_sum_level"),
        F.count(F.lit(1)).over(w_level).alias("rows_in_level"),
    )
)

top_rows = (
    metrics
    .filter(F.col("level_rank") <= 5)
    .drop("level_rank")
)

per_group = (
    top_rows
    .groupBy(
        F.when(F.col("rows_in_level") >= 11, "large").when(F.col("rows_in_level") >= 9, "medium").otherwise("small").alias("rows_in_level_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("adjuster_name").isNotNull(), 1).otherwise(0)).alias("n_adjuster_name_is_not_null"),
        F.max("level_share_of_level").alias("max_level_share_of_level"),
        F.array_sort(F.collect_set("team")).alias("team_list"),
        F.countDistinct("team").alias("distinct_teams"),
    )
    .filter(F.col("cnt") >= 3)
)

with_trends = (
    per_group
    .select(
        "rows_in_level_tier",
        "cnt",
        "n_adjuster_name_is_not_null",
        "max_level_share_of_level",
        "distinct_teams",
        F.size(F.collect_set("rows_in_level_tier").over(w_n_adjuster_name_is_not_null)).alias("distinct_rows_in_level_tier_in_n_adjuster_name_i"),
        F.row_number().over(w_rows_in_level_tier_by_distin).alias("rn"),
        F.nth_value("cnt", 2).over(w_rows_in_level_tier_by_distin_2).alias("second_cnt"),
        F.min("distinct_teams").over(w_n_adjuster_name_is_not_null).alias("n_adjuster_name_is_not_null_min_distinct_teams"),
    )
)

result = (
    with_trends
    .select(
        "rows_in_level_tier",
        "cnt",
        "n_adjuster_name_is_not_null",
        "max_level_share_of_level",
        "distinct_teams",
        "distinct_rows_in_level_tier_in_n_adjuster_name_i",
        "second_cnt",
        "n_adjuster_name_is_not_null_min_distinct_teams",
        F.lower(F.regexp_replace(F.trim(F.col("rows_in_level_tier")), " ", "_")).alias("rows_in_level_tier_slug"),
        F.coalesce(F.col("rows_in_level_tier"), F.lit("UNKNOWN")).alias("rows_in_level_tier_or_unknown"),
    )
    .orderBy(F.col("n_adjuster_name_is_not_null").asc_nulls_last())
)
