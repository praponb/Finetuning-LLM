from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_level_distance_full = Window.orderBy("level_distance", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_level_adjuster_name_len = Window.partitionBy("level", "adjuster_name_len")

scoped = (
    df_adjusters
    .filter(F.col("level") > 2)
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.coalesce(F.col("team"), F.lit("UNKNOWN")).alias("team_or_unknown"),
        F.abs(F.col("level") - 3).alias("level_distance"),
        F.length(F.trim(F.col("adjuster_name"))).alias("adjuster_name_len"),
    )
)

sequenced = (
    scoped
    .select(
        "*",
        F.nth_value("level_distance", 2).over(w_all_by_level_distance_full).alias("second_level_distance"),
        F.sum("level_distance").over(w_level_adjuster_name_len).alias("level_adjuster_name_len_sum_level_distance"),
        F.round(F.col("level_distance") / F.nullif(F.sum("level_distance").over(w_level_adjuster_name_len), F.lit(0)), 4).alias("level_distance_share_of_level_adjuster_name_len"),
        F.count(F.lit(1)).over(w_level_adjuster_name_len).alias("rows_in_level_adjuster_name_len"),
    )
)

without_matches = (
    sequenced.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.id")) & (F.col("c.policy_id").isNotNull()), "left_anti")
    .filter(F.length(F.trim(F.col("adjuster_name"))) >= 5)
)

result = (
    without_matches
    .filter(F.col("level_distance") >= 1)
    .select(
        "id",
        "adjuster_name",
        "team_or_unknown",
        "level_distance",
        "adjuster_name_len",
        "level_adjuster_name_len_sum_level_distance",
        "level_distance_share_of_level_adjuster_name_len",
    )
    .orderBy(
        F.col("level_distance").desc_nulls_first(),
        F.col("level_distance_share_of_level_adjuster_name_len").asc_nulls_last(),
    )
)
