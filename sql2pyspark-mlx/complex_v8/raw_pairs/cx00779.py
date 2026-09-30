from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_team_level = Window.partitionBy("team", "level")
w_team_level_distance_by_level = Window.partitionBy("team", "level_distance").orderBy(F.col("level").desc(), "id").rowsBetween(-2, Window.currentRow)
w_level_claim_claim_type_count = Window.partitionBy("level", "claim_claim_type_count").orderBy("claim_claim_type_count")
w_distinct_team_group_in_team = Window.partitionBy("distinct_team_group_in_team_level").orderBy(F.col("distinct_team_group_in_team_level").asc_nulls_last(), "id")
w_level_claim_claim_type_count_2 = Window.partitionBy("level", "claim_claim_type_count").orderBy("claim_claim_type_count", "id").rowsBetween(-6, Window.currentRow)
w_team_group = Window.partitionBy("team_group")
w_level_claim_claim_type_count_3 = Window.partitionBy("level", "claim_claim_type_count").orderBy("claim_claim_type_count", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_level_by_n_adjuster_name_is = Window.partitionBy("level").orderBy(F.col("n_adjuster_name_is_not_null").desc(), "level", "team", "claim_claim_type_count_dense_rank").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_level_team = Window.partitionBy("level", "team")
w_claim_claim_type_count_dense = Window.partitionBy("claim_claim_type_count_dense_rank").orderBy(F.col("n_adjuster_name_is_not_null").desc(), "level", "team", "claim_claim_type_count_dense_rank").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_level_by_n_adjuster_name_is_2 = Window.partitionBy("level").orderBy(F.col("n_adjuster_name_is_not_null").desc(), "level", "team", "claim_claim_type_count_dense_rank").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_adjusters
    .filter((F.lower(F.col("adjuster_name")).like("% g%")) & (F.col("level") < 3))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
    )
)

labeled = (
    filtered
    .filter(F.col("level") >= 1)
    .select(
        "*",
        F.abs(F.col("level") - 3).alias("level_distance"),
        F.substring(F.trim(F.col("adjuster_name")), 1, 2).alias("adjuster_name_prefix"),
        F.when(F.col("team").isin("Travel", "Health"), "group_a").when(F.col("team") == "Property", "group_b").otherwise("other").alias("team_group"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.count(F.when(F.col("claim_type") == "fire", 1)).alias("claim_claim_type_count"),
    )
)

profiled = (
    labeled.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
    )
)

sequenced = (
    profiled
    .select(
        "*",
        F.size(F.collect_set("team_group").over(w_team_level)).alias("distinct_team_group_in_team_level"),
        F.sum("level").over(w_team_level_distance_by_level).alias("level_sum_3"),
    )
)

ranked = (
    sequenced
    .select(
        "*",
        F.dense_rank().over(w_level_claim_claim_type_count).alias("claim_claim_type_count_dense_rank"),
        F.round(F.cume_dist().over(w_distinct_team_group_in_team), 3).alias("distinct_team_group_in_team_level_cume_dist"),
        F.sum("claim_claim_type_count").over(w_level_claim_claim_type_count_2).alias("claim_claim_type_count_sum_7"),
        F.round(F.col("level_distance") / F.nullif(F.sum("level_distance").over(w_team_group), F.lit(0)), 4).alias("level_distance_share_of_team_group"),
        F.nth_value("claim_claim_type_count", 2).over(w_level_claim_claim_type_count_3).alias("second_claim_claim_type_count"),
    )
)

per_group = (
    ranked
    .groupBy("level", "team", "claim_claim_type_count_dense_rank")
    .agg(
        F.array_sort(F.collect_set("adjuster_name")).alias("adjuster_name_list"),
        F.count(F.when(F.col("adjuster_name").isNotNull(), 1)).alias("n_adjuster_name_is_not_null"),
        F.avg("second_claim_claim_type_count").alias("avg_second_claim_claim_type_count_check"),
    )
    .filter(F.col("avg_second_claim_claim_type_count_check") > 2)
    .drop("avg_second_claim_claim_type_count_check")
)

metrics = (
    per_group
    .select(
        "*",
        F.nth_value("level", 2).over(w_level_by_n_adjuster_name_is).alias("second_level"),
        F.round(F.col("level") - F.avg("level").over(w_level_team), 2).alias("level_vs_level_team_avg"),
        F.nth_value("level", 2).over(w_claim_claim_type_count_dense).alias("second_level_2"),
        F.min("level").over(w_level_team).alias("level_team_min_level"),
        F.last("level", ignorenulls=True).over(w_level_by_n_adjuster_name_is_2).alias("last_known_level"),
        F.first("level", ignorenulls=True).over(w_level_by_n_adjuster_name_is_2).alias("first_known_level"),
    )
)

result = (
    metrics
    .groupingSets(
        [["first_known_level", "second_level_2"], ["first_known_level"]],
        "first_known_level", "second_level_2",
    )
    .agg(
        F.grouping("first_known_level").alias("is_total_first_known_level"),
        F.array_sort(F.collect_set("team")).alias("team_list"),
    )
    .orderBy(
        F.col("first_known_level").asc_nulls_last(),
        F.col("second_level_2").asc_nulls_last(),
    )
)
