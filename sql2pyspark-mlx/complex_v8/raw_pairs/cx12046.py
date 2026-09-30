from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_level_team_by_level_ties = Window.partitionBy("level", "team").orderBy("level")
w_level_team_by_level_prior_1 = Window.partitionBy("level", "team").orderBy("level", "id").rowsBetween(-1, -1)
w_level_team_by_level = Window.partitionBy("level", "team").orderBy("level", "id")
w_team = Window.partitionBy("team")
w_all_by_level_running = Window.orderBy("level", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_adjusters = (
    df_adjusters
    .filter(F.col("adjuster_name").isNotNull())
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.concat(F.col("adjuster_name"), F.lit("#"), F.col("level").cast("string")).alias("adjuster_name_ref"),
        F.initcap(F.trim(F.col("adjuster_name"))).alias("adjuster_name_clean"),
        F.substring(F.trim(F.col("team")), 1, 4).alias("team_prefix"),
    )
)

with_windows = (
    prepared_adjusters
    .select(
        "*",
        F.round(F.percent_rank().over(w_level_team_by_level_ties), 3).alias("level_percent_rank"),
        F.sum("level").over(w_level_team_by_level_prior_1).alias("level_sum_1_prior"),
        F.lag("level").over(w_level_team_by_level).alias("prev_level"),
        F.last("level", ignorenulls=True).over(w_all_by_level_running).alias("last_known_level"),
    )
)

result = (
    with_windows
    .groupingSets(
        [["last_known_level", "team"], ["last_known_level"], ["team"], []],
        "last_known_level", "team",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.max("level").alias("max_level"),
        F.round(100 * F.count(F.when(F.upper(F.trim(F.col("adjuster_name"))).like("G%"), 1)) / F.count("*"), 1).alias("pct_upper_trim_adjuster_name_lik"),
    )
    .select(
        "last_known_level",
        F.coalesce(F.col("team"), F.lit("ALL")).alias("team"),
        "grouping_level",
        "max_level",
        "pct_upper_trim_adjuster_name_lik",
    )
)
