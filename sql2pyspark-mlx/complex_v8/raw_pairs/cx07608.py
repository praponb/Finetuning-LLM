from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_level = Window.partitionBy("level")
w_team_or_unknown_by_level_tie = Window.partitionBy("team_or_unknown").orderBy(F.col("level").desc())

cleaned_adjusters = (
    df_adjusters
    .filter(~F.col("team").isin("Property", "Travel"))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.substring(F.trim(F.col("adjuster_name")), 1, 2).alias("adjuster_name_prefix"),
        F.coalesce(F.col("team"), F.lit("UNKNOWN")).alias("team_or_unknown"),
        F.coalesce(F.col("team"), F.lit("UNKNOWN")).alias("team_or_unknown_2"),
    )
)

screened = (
    cleaned_adjusters
    .crossJoin(cleaned_adjusters.agg(F.avg("level").alias("overall_avg_level")))
    .filter(F.col("level") > F.col("overall_avg_level"))
    .drop("overall_avg_level")
)

with_trends = (
    screened
    .select(
        "id",
        "adjuster_name",
        "level",
        "adjuster_name_prefix",
        "team_or_unknown",
        "team_or_unknown_2",
        F.round(F.col("level") / F.nullif(F.sum("level").over(w_level), F.lit(0)), 4).alias("level_share_of_level"),
        F.round(F.cume_dist().over(w_team_or_unknown_by_level_tie), 3).alias("level_cume_dist"),
        F.count(F.lit(1)).over(w_level).alias("rows_in_level"),
    )
)

result = (
    with_trends
    .cube("team_or_unknown_2", "team_or_unknown")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.array_sort(F.collect_set("adjuster_name")).alias("adjuster_name_list"),
        F.count("*").alias("row_count"),
    )
)
