from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tier_team = Window.partitionBy("tier", "team")
w_team_by_days_since_hired_on = Window.partitionBy("team").orderBy("days_since_hired_on_2", "id")
w_team_by_days_since_hired_on_2 = Window.partitionBy("team").orderBy("days_since_hired_on_2")
w_team_tier_by_hired_on = Window.partitionBy("team", "tier").orderBy("hired_on", "id")

valid = (
    df_support_agents
    .filter((F.col("team") == "VIP") & (F.col("tier").isin("T3", "T1")))
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("hired_on")).alias("days_since_hired_on"),
        F.date_format(F.col("hired_on"), "MMM yyyy").alias("hired_on_label"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("hired_on")).alias("days_since_hired_on_2"),
    )
)

team_stats = (
    valid
    .groupBy("team")
    .agg(F.avg("days_since_hired_on_2").alias("avg_days_since_hired_on_2"))
)

outliers = (
    valid.alias("s")
    .join(team_stats.alias("g"), F.col("g.team") == F.col("s.team"), "inner")
    .filter(F.col("s.days_since_hired_on_2") > F.col("g.avg_days_since_hired_on_2"))
    .select(
        "s.*",
        "g.avg_days_since_hired_on_2",
        F.round(F.col("s.days_since_hired_on_2") - F.col("g.avg_days_since_hired_on_2"), 2).alias("days_since_hired_on_2_minus_avg"),
    )
)

sequenced = (
    outliers
    .select(
        "id",
        "team",
        "tier",
        "hired_on",
        "hired_on_label",
        "days_since_hired_on_2",
        F.lag("avg_days_since_hired_on_2").over(w_team_by_days_since_hired_on).alias("prev_avg_days_since_hired_on_2"),
        F.round(F.percent_rank().over(w_team_by_days_since_hired_on_2), 3).alias("days_since_hired_on_2_percent_rank"),
        F.round(F.col("days_since_hired_on") - F.avg("days_since_hired_on").over(w_tier_team), 2).alias("days_since_hired_on_vs_tier_team_avg"),
        F.lag("hired_on").over(w_team_tier_by_hired_on).alias("prev_hired_on"),
        F.count(F.lit(1)).over(w_tier_team).alias("rows_in_tier_team"),
    )
)

summary = (
    sequenced
    .groupBy(
        "tier",
        "team",
        F.date_trunc("month", "prev_hired_on").alias("prev_hired_on_month"),
    )
    .agg(
        F.array_sort(F.collect_set("tier")).alias("tier_list"),
        F.min("prev_hired_on").alias("first_prev_hired_on"),
        F.count("*").alias("row_count"),
    )
)

result = (
    summary
    .filter(F.col("prev_hired_on_month") < F.lit("2025-02-05 09:00:00").cast("timestamp"))
    .select("tier", "prev_hired_on_month", "tier_list")
    .orderBy(F.col("tier").desc())
)
