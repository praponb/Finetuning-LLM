from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_level_full = Window.orderBy(F.col("level").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_level_by_level = Window.partitionBy("level").orderBy(F.col("level").desc(), "id")
w_all_by_adjuster_name_clean_l = Window.orderBy(F.col("adjuster_name_clean_len").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned = (
    df_adjusters
    .filter(F.upper(F.trim(F.col("adjuster_name"))).like("F%"))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.lower(F.trim(F.col("adjuster_name"))).alias("adjuster_name_clean"),
    )
)

enriched = (
    cleaned
    .select(
        "*",
        F.when(F.col("team").isin("Health", "Motor"), "group_a").when(F.col("team") == "Travel", "group_b").otherwise("other").alias("team_group"),
        F.concat(F.col("adjuster_name_clean"), F.lit("#"), F.col("level").cast("string")).alias("adjuster_name_clean_ref"),
        F.length(F.trim(F.col("adjuster_name_clean"))).alias("adjuster_name_clean_len"),
    )
)

with_trends = (
    enriched
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        "adjuster_name_clean",
        "team_group",
        "adjuster_name_clean_ref",
        "adjuster_name_clean_len",
        F.last("level").over(w_all_by_level_full).alias("last_level"),
        F.dense_rank().over(w_level_by_level).alias("level_dense_rank"),
        F.first("level").over(w_all_by_adjuster_name_clean_l).alias("first_level"),
    )
)

tagged = (
    with_trends
    .select(
        "*",
        F.coalesce(F.col("team_group"), F.lit("UNKNOWN")).alias("team_group_or_unknown"),
        F.least(F.col("adjuster_name_clean_len"), F.col("level_dense_rank")).alias("least_adjuster_name_clean_len_level_dense_rank"),
    )
)

result = (
    tagged
    .rollup("team_group", "team")
    .agg(
        F.grouping("team_group").alias("is_total_team_group"),
        F.count("*").alias("n_rows"),
    )
    .select(
        F.coalesce(F.col("team_group"), F.lit("ALL")).alias("team_group"),
        F.coalesce(F.col("team"), F.lit("ALL")).alias("team"),
        "is_total_team_group",
        "n_rows",
    )
    .orderBy(F.col("team_group").asc_nulls_last(), F.col("team").asc_nulls_last())
)
