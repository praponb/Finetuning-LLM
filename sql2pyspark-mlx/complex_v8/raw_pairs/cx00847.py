from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_team_by_hired_on = Window.partitionBy("team").orderBy("hired_on", "id")
w_team_by_hired_on_running = Window.partitionBy("team").orderBy("hired_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_hired_on = Window.orderBy("hired_on", "id")
w_team_by_hired_on_full = Window.partitionBy("team").orderBy("hired_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_tier_group_hired_on_quintile = Window.partitionBy("tier_group", "hired_on_quintile").orderBy(F.col("median_hired_on_quartile").asc_nulls_last(), "hired_on_ym", "tier_group", "hired_on_quintile").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hired_on_quintile = Window.partitionBy("hired_on_quintile")
w_all_by_hired_on_quartile_tie = Window.orderBy("hired_on_quartile_tier_is_not_null", "hired_on_ym", "tier_group", "hired_on_quintile").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_tier_group_hired_on_quintile_2 = Window.partitionBy("tier_group", "hired_on_quintile").orderBy(F.col("median_hired_on_quartile").asc_nulls_last(), "hired_on_ym", "tier_group", "hired_on_quintile").rowsBetween(-6, -1)
w_all_by_hired_on_quartile_tie_2 = Window.orderBy("hired_on_quartile_tier_is_not_null", "hired_on_ym", "tier_group", "hired_on_quintile").rowsBetween(-1, Window.currentRow)

prepared = (
    df_support_agents
    .filter((F.col("hired_on") < F.lit("2025-02-26").cast("date")) & (F.lower(F.col("agent_name")).like("%ri%")))
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
        F.when(F.col("tier").isin("T1", "T3"), "group_a").when(F.col("tier") == "T2", "group_b").otherwise("other").alias("tier_group"),
        F.coalesce(F.col("tier"), F.lit("UNKNOWN")).alias("tier_or_unknown"),
    )
)

metrics = (
    prepared
    .select(
        "*",
        F.ntile(4).over(w_team_by_hired_on).alias("hired_on_quartile"),
        F.first("hired_on", ignorenulls=True).over(w_team_by_hired_on_running).alias("first_known_hired_on"),
        F.ntile(5).over(w_team_by_hired_on).alias("hired_on_quintile"),
        F.datediff(F.col("hired_on"), F.lag("hired_on").over(w_all_by_hired_on)).alias("days_since_prev_hired_on"),
        F.round(F.percent_rank().over(w_all_by_hired_on), 3).alias("hired_on_percent_rank"),
        F.nth_value("hired_on", 2).over(w_team_by_hired_on_full).alias("second_hired_on"),
    )
)

grouped = (
    metrics
    .groupBy(
        F.date_format("hired_on", "yyyy-MM").alias("hired_on_ym"),
        "tier_group",
        "hired_on_quintile",
    )
    .agg(
        F.sum(F.when(F.col("tier").isNotNull(), F.col("hired_on_quartile")).otherwise(0)).alias("hired_on_quartile_tier_is_not_null"),
        F.percentile_approx("hired_on_quartile", 0.5).alias("median_hired_on_quartile"),
        F.round(F.stddev("hired_on_quartile"), 2).alias("stddev_hired_on_quartile"),
        F.round(F.stddev("hired_on_quintile"), 2).alias("stddev_hired_on_quintile"),
        F.sum("days_since_prev_hired_on").alias("sum_days_since_prev_hired_on_check"),
    )
    .filter(F.col("sum_days_since_prev_hired_on_check") > 50)
    .drop("sum_days_since_prev_hired_on_check")
)

ranked = (
    grouped
    .select(
        "hired_on_ym",
        "tier_group",
        "hired_on_quintile",
        "stddev_hired_on_quartile",
        "stddev_hired_on_quintile",
        F.last("hired_on_quintile", ignorenulls=True).over(w_tier_group_hired_on_quintile).alias("last_known_hired_on_quintile"),
        F.count(F.lit(1)).over(w_hired_on_quintile).alias("rows_in_hired_on_quintile"),
        F.min("stddev_hired_on_quartile").over(w_hired_on_quintile).alias("hired_on_quintile_min_stddev_hired_on_quartile"),
        F.first("hired_on_quintile").over(w_all_by_hired_on_quartile_tie).alias("first_hired_on_quintile"),
        F.max("median_hired_on_quartile").over(w_tier_group_hired_on_quintile_2).alias("median_hired_on_quartile_max_6_prior"),
        F.sum("median_hired_on_quartile").over(w_all_by_hired_on_quartile_tie_2).alias("median_hired_on_quartile_sum_2"),
    )
)

result = (
    ranked
    .select(
        "hired_on_ym",
        "tier_group",
        "first_hired_on_quintile",
        "median_hired_on_quartile_max_6_prior",
        "median_hired_on_quartile_sum_2",
    )
    .unpivot(
        ids=["hired_on_ym", "tier_group"],
        values=["first_hired_on_quintile", "median_hired_on_quartile_max_6_prior", "median_hired_on_quartile_sum_2"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("hired_on_ym", "tier_group", "metric_name")
)
