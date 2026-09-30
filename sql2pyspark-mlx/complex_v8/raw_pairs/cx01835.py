from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hired_on_year_team_by_hired = Window.partitionBy(F.year("hired_on"), "team").orderBy(F.col("hired_on_month_start").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hired_on_year_team_by_hired_2 = Window.partitionBy(F.year("hired_on"), "team").orderBy(F.col("hired_on_month_start").desc())
w_hired_on_year_team_by_hired_3 = Window.partitionBy(F.year("hired_on"), "team").orderBy(F.col("hired_on_month_start").desc(), "id")

valid = (
    df_support_agents
    .filter(F.length(F.trim(F.col("agent_name"))) >= 5)
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
        F.trunc(F.col("hired_on"), "MM").alias("hired_on_month_start"),
    )
)

sequenced = (
    valid
    .select(
        "*",
        F.first("hired_on", ignorenulls=True).over(w_hired_on_year_team_by_hired).alias("first_known_hired_on"),
        F.round(F.cume_dist().over(w_hired_on_year_team_by_hired_2), 3).alias("hired_on_month_start_cume_dist"),
        F.lag("hired_on").over(w_hired_on_year_team_by_hired_3).alias("prev_hired_on"),
        F.datediff(F.col("hired_on"), F.lag("hired_on").over(w_hired_on_year_team_by_hired_3)).alias("days_since_prev_hired_on"),
        F.lag("hired_on_month_start", 3).over(w_hired_on_year_team_by_hired_3).alias("prev3_hired_on_month_start"),
        F.round(F.percent_rank().over(w_hired_on_year_team_by_hired_3), 3).alias("hired_on_month_start_percent_rank"),
    )
)

result = (
    sequenced
    .rollup("tier", "team")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.sum(F.when(F.col("tier").isNotNull(), 1).otherwise(0)).alias("n_tier_is_not_null"),
        F.countDistinct("team").alias("distinct_teams"),
        F.count("*").alias("n_rows"),
    )
    .select(
        F.coalesce(F.col("tier"), F.lit("ALL")).alias("tier"),
        F.coalesce(F.col("team"), F.lit("ALL")).alias("team"),
        "grouping_level",
        "n_tier_is_not_null",
        "distinct_teams",
        "n_rows",
    )
    .orderBy(F.col("tier").asc_nulls_last(), F.col("team").asc_nulls_last())
)
