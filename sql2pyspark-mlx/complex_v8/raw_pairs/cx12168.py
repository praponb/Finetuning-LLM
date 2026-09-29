from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_cnt_by_distinct_level_tiers = Window.partitionBy("cnt").orderBy("distinct_level_tiers", "level", "level_tier")
w_level_distinct_level_tiers = Window.partitionBy("level", "distinct_level_tiers")
w_cnt_by_distinct_level_tiers_2 = Window.partitionBy("cnt").orderBy("distinct_level_tiers", "level", "level_tier").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid = (
    df_adjusters
    .filter((F.upper(F.trim(F.col("adjuster_name"))).like("F%")) & (F.col("level") <= 3))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.concat(F.col("adjuster_name"), F.lit("#"), F.col("level").cast("string")).alias("adjuster_name_ref"),
        F.when(F.col("level") >= 3, "HIGH").when(F.col("level") >= 2, "MEDIUM").otherwise("LOW").alias("level_tier"),
        F.length(F.trim(F.col("adjuster_name"))).alias("adjuster_name_len"),
    )
)

rollup_base = (
    valid
    .groupBy("level", "level_tier")
    .agg(
        F.countDistinct("level_tier").alias("distinct_level_tiers"),
        F.sum("adjuster_name_len").alias("total_adjuster_name_len"),
        F.count("*").alias("cnt"),
        F.countDistinct("team").alias("distinct_teams"),
        F.avg("adjuster_name_len").alias("avg_adjuster_name_len_check"),
    )
    .filter(F.col("avg_adjuster_name_len_check") > 5)
    .drop("avg_adjuster_name_len_check")
)

with_windows = (
    rollup_base
    .select(
        "level",
        "level_tier",
        "cnt",
        "distinct_teams",
        F.dense_rank().over(w_cnt_by_distinct_level_tiers).alias("distinct_level_tiers_dense_rank"),
        F.round(F.col("distinct_level_tiers") - F.avg("distinct_level_tiers").over(w_level_distinct_level_tiers), 2).alias("distinct_level_tiers_vs_level_distinct_level_tie"),
        F.first("distinct_teams").over(w_cnt_by_distinct_level_tiers_2).alias("first_distinct_teams"),
    )
)

leaders = with_windows.filter((F.col("distinct_level_tiers_dense_rank") <= 2) & (F.col("cnt") <= 10))

result = (
    leaders
    .groupBy("cnt", "level")
    .pivot("level_tier", ["LOW", "MEDIUM"])
    .agg(F.count("distinct_level_tiers_dense_rank"))
)
