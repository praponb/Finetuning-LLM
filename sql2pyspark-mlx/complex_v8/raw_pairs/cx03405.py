from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("team").orderBy(F.col("level").desc(), F.col("id").desc())
w_all_by_level_ties = Window.orderBy(F.col("level").desc())
w_team_level = Window.partitionBy("team", "level")
w_team_level_by_level_running = Window.partitionBy("team", "level").orderBy(F.col("level").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_adjusters = (
    df_adjusters
    .filter(F.col("adjuster_name").isNotNull())
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
    )
)

latest = (
    base_adjusters
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_trends = (
    latest
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_level_ties), 3).alias("level_cume_dist"),
        F.round(F.col("level") / F.nullif(F.sum("level").over(w_team_level), F.lit(0)), 4).alias("level_share_of_team_level"),
        F.sum("level").over(w_team_level).alias("team_level_sum_level"),
        F.first("level", ignorenulls=True).over(w_team_level_by_level_running).alias("first_known_level"),
    )
)

labeled = (
    with_trends
    .filter(F.col("team").isin("Health", "Motor", "Travel"))
    .select(
        "*",
        F.round(F.col("level") / F.nullif(F.col("level_cume_dist"), F.lit(0)), 3).alias("level_per_level_cume_dist"),
        F.coalesce(F.col("team"), F.lit("UNKNOWN")).alias("team_or_unknown"),
    )
)

screened = (
    labeled
    .crossJoin(labeled.agg(F.avg("level").alias("overall_avg_level")))
    .filter(F.col("level") > F.col("overall_avg_level"))
    .drop("overall_avg_level")
)

result = (
    screened
    .select(
        "id",
        "adjuster_name",
        "level_share_of_team_level",
        "team_level_sum_level",
        "first_known_level",
        "level_per_level_cume_dist",
        F.upper(F.trim(F.col("team_or_unknown"))).alias("team_or_unknown_clean"),
    )
    .orderBy(F.col("adjuster_name").asc_nulls_last(), "id")
    .limit(5)
)
