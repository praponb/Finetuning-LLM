from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hired_on_quarter = Window.partitionBy("hired_on_quarter")
w_team_tier = Window.partitionBy("team", "tier")
w_all_by_days_since_hired_on_p = Window.orderBy("days_since_hired_on", "id").rowsBetween(-2, -1)
w_team_by_hired_on_moving_2 = Window.partitionBy("team").orderBy("hired_on", "id").rowsBetween(-2, Window.currentRow)
w_hired_on_month_by_days_since = Window.partitionBy(F.month("hired_on")).orderBy(F.col("days_since_hired_on").asc_nulls_last(), "id").rowsBetween(-3, -1)
w_team_by_hired_on = Window.partitionBy("team").orderBy("hired_on", "id")
w_hired_on_month_by_days_since_2 = Window.partitionBy(F.month("hired_on")).orderBy(F.col("days_since_hired_on").asc_nulls_last(), "id")
w_team_by_hired_on_prior_2 = Window.partitionBy("team").orderBy("hired_on", "id").rowsBetween(-2, -1)
w_prev3_hired_on_quarter_hired = Window.partitionBy("prev3_hired_on_quarter", "hired_on_rank").orderBy("rows_in_team_tier", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_team_hired_on_quarter_by_day = Window.partitionBy("team", "hired_on_quarter").orderBy("days_since_hired_on_max_2_prior", "id")

recent = (
    df_support_agents
    .filter((F.col("hired_on").isNotNull()) & (F.col("tier").isin("T1", "T3")))
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
        F.quarter(F.col("hired_on")).alias("hired_on_quarter"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("hired_on")).alias("days_since_hired_on"),
    )
)

ranked = (
    recent
    .select(
        "*",
        F.count(F.lit(1)).over(w_hired_on_quarter).alias("rows_in_hired_on_quarter"),
        F.count(F.lit(1)).over(w_team_tier).alias("rows_in_team_tier"),
        F.max("days_since_hired_on").over(w_all_by_days_since_hired_on_p).alias("days_since_hired_on_max_2_prior"),
        F.round(F.avg("days_since_hired_on").over(w_team_by_hired_on_moving_2), 2).alias("days_since_hired_on_avg_3"),
        F.round(F.avg("hired_on_quarter").over(w_hired_on_month_by_days_since), 2).alias("hired_on_quarter_avg_3_prior"),
        F.rank().over(w_team_by_hired_on).alias("hired_on_rank"),
        F.max("days_since_hired_on").over(w_team_tier).alias("team_tier_max_days_since_hired_on"),
        F.lag("hired_on_quarter", 3, 0).over(w_hired_on_month_by_days_since_2).alias("prev3_hired_on_quarter"),
        F.sum("days_since_hired_on").over(w_team_by_hired_on_prior_2).alias("days_since_hired_on_sum_2_prior"),
    )
)

enriched = ranked.withColumn("hired_on_quarter_2", F.quarter(F.col("hired_on")))

sequenced = (
    enriched
    .select(
        "id",
        "agent_name",
        "hired_on_quarter",
        "days_since_hired_on",
        "rows_in_hired_on_quarter",
        "days_since_hired_on_avg_3",
        "hired_on_quarter_avg_3_prior",
        "team_tier_max_days_since_hired_on",
        "prev3_hired_on_quarter",
        "hired_on_quarter_2",
        F.max("days_since_hired_on_avg_3").over(w_prev3_hired_on_quarter_hired).alias("max_days_since_hired_on_avg_3_so_far"),
        (F.col("days_since_hired_on") - F.lag("days_since_hired_on").over(w_team_hired_on_quarter_by_day)).alias("days_since_hired_on_change"),
        (F.col("days_since_hired_on_avg_3") - F.lag("days_since_hired_on_avg_3").over(w_team_hired_on_quarter_by_day)).alias("days_since_hired_on_avg_3_change"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "agent_name",
        "rows_in_hired_on_quarter",
        "hired_on_quarter_avg_3_prior",
        "team_tier_max_days_since_hired_on",
        "prev3_hired_on_quarter",
        "hired_on_quarter_2",
        "max_days_since_hired_on_avg_3_so_far",
        "days_since_hired_on_change",
        "days_since_hired_on_avg_3_change",
        (F.col("rows_in_hired_on_quarter") - F.col("hired_on_quarter")).alias("rows_in_hired_on_quarter_minus_hired_on_quarter"),
    )
    .orderBy(
        "days_since_hired_on_change",
        F.col("days_since_hired_on_avg_3_change").desc(),
    )
)
