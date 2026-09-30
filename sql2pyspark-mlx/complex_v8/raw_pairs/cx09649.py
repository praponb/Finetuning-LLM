from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_hired_on_ties = Window.orderBy(F.col("hired_on").desc())
w_all_by_hired_on_full = Window.orderBy(F.col("hired_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_hired_on = Window.orderBy(F.col("hired_on").desc(), "id")
w_second_hired_on_month_tier_b = Window.partitionBy(F.month("second_hired_on"), "tier").orderBy("second_hired_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hired_on_month_tier = Window.partitionBy(F.month("hired_on"), "tier")
w_all_by_second_hired_on_full = Window.orderBy("second_hired_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_hired_on_tercile = Window.partitionBy("hired_on_tercile")
w_hired_on_tercile_by_second_h = Window.partitionBy("hired_on_tercile").orderBy(F.col("second_hired_on_quarter").asc_nulls_last(), "id")
w_hired_on_tercile_by_hired_on = Window.partitionBy("hired_on_tercile").orderBy(F.col("hired_on_percent_rank").asc_nulls_last())
w_hired_on_tercile_by_hired_on_2 = Window.partitionBy("hired_on_tercile").orderBy(F.col("hired_on_percent_rank").asc_nulls_last(), "id")
w_hired_on_tercile_by_second_h_2 = Window.partitionBy("hired_on_tercile").orderBy(F.col("second_hired_on_quarter").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned = (
    df_support_agents
    .filter((F.col("team").isin("Onboarding", "VIP")) & (F.upper(F.trim(F.col("agent_name"))).like("L%")))
    .select(
        "id",
        "team",
        "tier",
        "hired_on",
    )
)

windowed = (
    cleaned
    .select(
        "id",
        "tier",
        "hired_on",
        F.round(F.cume_dist().over(w_all_by_hired_on_ties), 3).alias("hired_on_cume_dist"),
        F.round(F.percent_rank().over(w_all_by_hired_on_ties), 3).alias("hired_on_percent_rank"),
        F.nth_value("hired_on", 2).over(w_all_by_hired_on_full).alias("second_hired_on"),
        F.ntile(3).over(w_all_by_hired_on).alias("hired_on_tercile"),
        F.datediff(F.col("hired_on"), F.lag("hired_on").over(w_all_by_hired_on)).alias("days_since_prev_hired_on"),
    )
)

windowed_2 = (
    windowed
    .select(
        "id",
        "hired_on_cume_dist",
        "hired_on_percent_rank",
        "second_hired_on",
        "hired_on_tercile",
        F.max("hired_on_percent_rank").over(w_second_hired_on_month_tier_b).alias("max_hired_on_percent_rank_so_far"),
        F.round(F.col("hired_on_percent_rank") / F.nullif(F.sum("hired_on_percent_rank").over(w_hired_on_month_tier), F.lit(0)), 4).alias("hired_on_percent_rank_share_of_hired_on_month_ti"),
        F.round(F.col("hired_on_cume_dist") - F.avg("hired_on_cume_dist").over(w_hired_on_month_tier), 2).alias("hired_on_cume_dist_vs_hired_on_month_tier_avg"),
        F.last("days_since_prev_hired_on").over(w_all_by_second_hired_on_full).alias("last_days_since_prev_hired_on"),
        F.round(F.avg("hired_on_tercile").over(w_hired_on_month_tier), 2).alias("hired_on_month_tier_avg_hired_on_tercile"),
        F.max("days_since_prev_hired_on").over(w_hired_on_month_tier).alias("hired_on_month_tier_max_days_since_prev_hired_on"),
    )
)

annotated = (
    windowed_2
    .select(
        "id",
        "hired_on_cume_dist",
        "hired_on_percent_rank",
        "hired_on_tercile",
        "hired_on_percent_rank_share_of_hired_on_month_ti",
        "hired_on_month_tier_avg_hired_on_tercile",
        "hired_on_month_tier_max_days_since_prev_hired_on",
        F.round(F.col("hired_on_percent_rank") / F.nullif(F.col("hired_on_tercile"), F.lit(0)), 3).alias("hired_on_percent_rank_per_hired_on_tercile"),
        F.date_trunc("quarter", F.col("second_hired_on")).alias("second_hired_on_quarter"),
    )
)

ranked = (
    annotated
    .select(
        "id",
        "hired_on_cume_dist",
        "hired_on_percent_rank",
        "hired_on_tercile",
        "hired_on_month_tier_max_days_since_prev_hired_on",
        "hired_on_percent_rank_per_hired_on_tercile",
        "second_hired_on_quarter",
        F.ntile(3).over(w_hired_on_tercile_by_second_h).alias("second_hired_on_quarter_tercile"),
        F.count(F.lit(1)).over(w_hired_on_tercile).alias("rows_in_hired_on_tercile"),
        F.rank().over(w_hired_on_tercile_by_hired_on).alias("hired_on_percent_rank_rank"),
        F.lag("second_hired_on_quarter").over(w_hired_on_tercile_by_hired_on_2).alias("prev_second_hired_on_quarter"),
        F.last("hired_on_cume_dist").over(w_hired_on_tercile_by_second_h_2).alias("last_hired_on_cume_dist"),
    )
)

result = (
    ranked
    .filter(F.col("hired_on_tercile") < 9)
    .select(
        "id",
        "hired_on_cume_dist",
        "hired_on_tercile",
        "second_hired_on_quarter_tercile",
        "rows_in_hired_on_tercile",
        "hired_on_percent_rank_rank",
        "prev_second_hired_on_quarter",
        F.when(F.col("hired_on_tercile") >= 6, "A").when(F.col("hired_on_tercile") >= 4, "B").otherwise("C").alias("hired_on_tercile_tier"),
    )
    .orderBy(
        "rows_in_hired_on_tercile",
        F.col("hired_on_tercile_tier").desc_nulls_first(),
    )
)
