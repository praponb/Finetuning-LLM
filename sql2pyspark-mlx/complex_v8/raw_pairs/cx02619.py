from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_level_ties = Window.orderBy(F.col("level").asc_nulls_last())
w_all_by_level_full = Window.orderBy(F.col("level").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_level_filled = Window.partitionBy("level_filled")
w_level_by_level_rank = Window.partitionBy("level").orderBy(F.col("level_rank").desc(), "id")
w_all_by_level_filled_max_leve = Window.orderBy(F.col("level_filled_max_level_filled").desc(), "id")

recent_adjusters = (
    df_adjusters
    .filter((~F.col("team").isin("Health", "Motor")) & (F.col("level") >= 1))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.coalesce(F.col("level"), F.lit(0)).alias("level_filled"),
    )
)

screened = (
    recent_adjusters.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.id")) & (F.col("c.claim_type") == "theft") & (F.col("c.policy_id") <= 26), "left_semi")
)

with_trends = (
    screened
    .select(
        "*",
        F.rank().over(w_all_by_level_ties).alias("level_rank"),
        F.nth_value("level_filled", 2).over(w_all_by_level_full).alias("second_level_filled"),
        F.max("level_filled").over(w_level_filled).alias("level_filled_max_level_filled"),
        F.sum("level_filled").over(w_level_filled).alias("level_filled_sum_level_filled"),
    )
)

derived = (
    with_trends
    .filter(F.col("level_rank") > 4)
    .select(
        "*",
        F.length(F.trim(F.col("adjuster_name"))).alias("adjuster_name_len"),
        F.round(F.col("level_filled") / F.nullif(F.col("level"), F.lit(0)), 3).alias("level_filled_per_level"),
        F.substring(F.trim(F.col("team")), 1, 3).alias("team_prefix"),
    )
)

with_windows = (
    derived
    .select(
        "*",
        F.lag("adjuster_name_len", 2).over(w_level_by_level_rank).alias("prev2_adjuster_name_len"),
        F.lead("level_filled", 2, 0).over(w_all_by_level_filled_max_leve).alias("next2_level_filled"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "adjuster_name",
        "team",
        "second_level_filled",
        "level_filled_max_level_filled",
        "level_filled_sum_level_filled",
        "team_prefix",
        "prev2_adjuster_name_len",
        "next2_level_filled",
        F.abs(F.col("level_filled") - 3).alias("level_filled_distance"),
        F.when(F.col("prev2_adjuster_name_len") >= 12, "large").when(F.col("prev2_adjuster_name_len") >= 6, "medium").otherwise("small").alias("prev2_adjuster_name_len_tier"),
    )
    .orderBy(
        F.col("team").asc_nulls_last(),
        F.col("prev2_adjuster_name_len_tier").desc(),
    )
)
