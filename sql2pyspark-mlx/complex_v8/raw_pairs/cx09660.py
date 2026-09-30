from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("team").orderBy(F.col("avg_level").desc(), F.col("id").desc())
w_team_by_adjuster_name_len_fu = Window.partitionBy("team").orderBy("adjuster_name_len", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_name_len_by_level_m = Window.partitionBy("adjuster_name_len").orderBy("level_minus_avg", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_team_by_adjuster_name_len = Window.partitionBy("team").orderBy("adjuster_name_len", "id")
w_adjuster_name_len_by_level_m_2 = Window.partitionBy("adjuster_name_len").orderBy("level_minus_avg", "id").rowsBetween(-6, -1)

cleaned = (
    df_adjusters
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.length(F.trim(F.col("adjuster_name"))).alias("adjuster_name_len"),
    )
)

team_stats = (
    cleaned
    .groupBy("team")
    .agg(F.avg("level").alias("avg_level"))
)

compared = (
    cleaned.alias("s")
    .join(team_stats.alias("g"), F.col("g.team") == F.col("s.team"), "inner")
    .filter(F.col("s.level") > F.col("g.avg_level"))
    .select(
        "s.*",
        "g.avg_level",
        F.round(F.col("s.level") - F.col("g.avg_level"), 2).alias("level_minus_avg"),
    )
)

latest_per_key = (
    compared
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

metrics = (
    latest_per_key
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.last("avg_level").over(w_team_by_adjuster_name_len_fu).alias("last_avg_level"),
        F.last("avg_level", ignorenulls=True).over(w_adjuster_name_len_by_level_m).alias("last_known_avg_level"),
        F.lag("level_minus_avg", 2).over(w_team_by_adjuster_name_len).alias("prev2_level_minus_avg"),
        F.dense_rank().over(w_team_by_adjuster_name_len).alias("adjuster_name_len_dense_rank"),
        F.round(F.avg("avg_level").over(w_adjuster_name_len_by_level_m_2), 2).alias("avg_level_avg_6_prior"),
        F.min("adjuster_name_len").over(w_adjuster_name_len_by_level_m).alias("min_adjuster_name_len_so_far"),
    )
)

grouped = (
    metrics
    .groupBy(
        F.when(F.col("prev2_level_minus_avg") >= 1.5, "A").when(F.col("prev2_level_minus_avg") >= -1.0, "B").otherwise("C").alias("prev2_level_minus_avg_tier"),
    )
    .agg(
        F.array_sort(F.collect_set("adjuster_name")).alias("adjuster_name_list"),
        F.round(F.sum("last_known_avg_level") / F.nullif(F.sum("prev2_level_minus_avg"), F.lit(0)), 3).alias("last_known_avg_level_to_prev2_level_minus_avg_ra"),
        F.round(F.stddev("level"), 2).alias("stddev_level"),
        F.min("prev2_level_minus_avg").alias("min_prev2_level_minus_avg"),
        F.round(F.sum("last_known_avg_level") / F.nullif(F.sum("last_avg_level"), F.lit(0)), 3).alias("last_known_avg_level_to_last_avg_level_ratio"),
        F.max("last_avg_level").alias("max_last_avg_level_check"),
    )
    .filter(F.col("max_last_avg_level_check") > 1.6)
    .drop("max_last_avg_level_check")
)

tagged = (
    grouped
    .select(
        "prev2_level_minus_avg_tier",
        "adjuster_name_list",
        "last_known_avg_level_to_prev2_level_minus_avg_ra",
        "stddev_level",
        "min_prev2_level_minus_avg",
        "last_known_avg_level_to_last_avg_level_ratio",
        F.round(F.col("stddev_level") * 1.5, 2).alias("stddev_level_adj"),
        F.round(F.col("stddev_level") * 1.5, 2).alias("stddev_level_adj_2"),
        F.abs(F.col("stddev_level") - 2.8).alias("stddev_level_distance"),
    )
)

labeled = (
    tagged
    .filter(F.col("stddev_level_distance") <= 3.5)
    .select(
        "*",
        F.round(F.col("stddev_level") / F.nullif(F.col("stddev_level_adj_2"), F.lit(0)), 3).alias("stddev_level_per_stddev_level_adj_2"),
        F.array_join(F.array_sort(F.col("adjuster_name_list")), ",").alias("adjuster_name_list_text"),
        F.coalesce(F.col("stddev_level"), F.lit(0.0)).alias("stddev_level_filled"),
    )
)

result = (
    labeled
    .select(
        "prev2_level_minus_avg_tier",
        "adjuster_name_list",
        "last_known_avg_level_to_prev2_level_minus_avg_ra",
        "stddev_level",
        "last_known_avg_level_to_last_avg_level_ratio",
        "stddev_level_adj_2",
        "stddev_level_distance",
        "stddev_level_per_stddev_level_adj_2",
        "adjuster_name_list_text",
        "stddev_level_filled",
    )
    .orderBy("stddev_level_filled")
)
