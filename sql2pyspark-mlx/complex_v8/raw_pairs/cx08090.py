from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_team_hired_on_month_month_by = Window.partitionBy("team", F.date_trunc("month", "hired_on_month")).orderBy("hired_on")
w_team_tier_by_hired_on = Window.partitionBy("team", "tier").orderBy("hired_on", "id")
w_tier_by_hired_on_month_full = Window.partitionBy("tier").orderBy(F.col("hired_on_month").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_tier_by_hired_on_month = Window.partitionBy("tier").orderBy(F.col("hired_on_month").desc(), "id")
w_hired_on_month_hired_on_mont = Window.partitionBy(F.month("hired_on"), F.to_date("hired_on_month")).orderBy("hired_on", "id")
w_all_by_hired_on_month_runnin = Window.orderBy("hired_on_month", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_team_tier_by_hired_on_ties = Window.partitionBy("team", "tier").orderBy("hired_on")
w_hired_on_month_year = Window.partitionBy(F.year("hired_on_month"))
w_next_hired_on_month_month_by = Window.partitionBy(F.month("next_hired_on_month")).orderBy("second_hired_on", "id").rowsBetween(-1, Window.currentRow)
w_next_hired_on_month_month = Window.partitionBy(F.month("next_hired_on_month"))

cleaned = (
    df_support_agents
    .filter((F.col("hired_on") < F.lit("2025-02-26").cast("date")) & (F.upper(F.trim(F.col("agent_name"))).like("J%")))
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
        F.trunc(F.col("hired_on"), "MM").alias("hired_on_month_start"),
        F.date_trunc("month", F.col("hired_on")).alias("hired_on_month"),
    )
)

ranked = (
    cleaned
    .select(
        "*",
        F.round(F.percent_rank().over(w_team_hired_on_month_month_by), 3).alias("hired_on_percent_rank"),
        F.datediff(F.col("hired_on_month_start"), F.lag("hired_on_month_start").over(w_team_tier_by_hired_on)).alias("days_since_prev_hired_on_month_start"),
        F.first("hired_on_month").over(w_tier_by_hired_on_month_full).alias("first_hired_on_month"),
        F.lead("hired_on_month").over(w_tier_by_hired_on_month).alias("next_hired_on_month"),
        F.nth_value("hired_on", 2).over(w_tier_by_hired_on_month_full).alias("second_hired_on"),
        F.lag("hired_on").over(w_hired_on_month_hired_on_mont).alias("prev_hired_on"),
        F.last("hired_on_month_start", ignorenulls=True).over(w_all_by_hired_on_month_runnin).alias("last_known_hired_on_month_start"),
        F.dense_rank().over(w_team_tier_by_hired_on_ties).alias("hired_on_dense_rank"),
        F.row_number().over(w_team_tier_by_hired_on).alias("seq_no"),
    )
)

tagged = (
    ranked
    .withColumn("first_hired_on_month_week", F.date_trunc("week", F.col("first_hired_on_month")))
)

metrics = (
    tagged
    .select(
        "*",
        F.min("hired_on_dense_rank").over(w_hired_on_month_year).alias("hired_on_month_year_min_hired_on_dense_rank"),
        F.round(F.avg("hired_on_percent_rank").over(w_next_hired_on_month_month_by), 2).alias("hired_on_percent_rank_avg_2"),
        F.round(F.col("hired_on_dense_rank") - F.avg("hired_on_dense_rank").over(w_hired_on_month_year), 2).alias("hired_on_dense_rank_vs_hired_on_month_year_avg"),
        F.round(F.col("hired_on_percent_rank") - F.avg("hired_on_percent_rank").over(w_next_hired_on_month_month), 2).alias("hired_on_percent_rank_vs_next_hired_on_month_mon"),
        F.round(F.col("seq_no") / F.nullif(F.sum("seq_no").over(w_next_hired_on_month_month), F.lit(0)), 4).alias("seq_no_share_of_next_hired_on_month_month"),
        F.round(F.col("hired_on_dense_rank") / F.nullif(F.sum("hired_on_dense_rank").over(w_hired_on_month_year), F.lit(0)), 4).alias("hired_on_dense_rank_share_of_hired_on_month_year"),
    )
)

result = (
    metrics
    .select(
        "id",
        "team",
        "tier",
        "hired_on",
        "hired_on_month_start",
        "hired_on_month",
        "first_hired_on_month",
        "next_hired_on_month",
        "second_hired_on",
        "prev_hired_on",
        "last_known_hired_on_month_start",
        "seq_no",
        "first_hired_on_month_week",
        "hired_on_percent_rank_avg_2",
        "hired_on_dense_rank_vs_hired_on_month_year_avg",
        "seq_no_share_of_next_hired_on_month_month",
        "hired_on_dense_rank_share_of_hired_on_month_year",
        F.datediff(F.col("second_hired_on"), F.col("hired_on")).alias("days_hired_on_to_second_hired_on"),
        F.year(F.col("prev_hired_on")).alias("prev_hired_on_year"),
    )
    .orderBy(F.col("days_hired_on_to_second_hired_on").desc_nulls_first())
)
