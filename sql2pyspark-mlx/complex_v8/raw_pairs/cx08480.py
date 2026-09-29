from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_team_by_hired_on_running = Window.partitionBy("team").orderBy(F.col("hired_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_team_by_hired_on_ties = Window.partitionBy("team").orderBy("hired_on")
w_team_by_hired_on_ties_2 = Window.partitionBy("team").orderBy(F.col("hired_on").desc())
w_team_by_hired_on = Window.partitionBy("team").orderBy("hired_on", "id")
w_all_by_hired_on_ties = Window.orderBy("hired_on")
w_all_by_hired_on = Window.orderBy("hired_on", "id")
w_team_by_hired_on_2 = Window.partitionBy("team").orderBy(F.col("hired_on").desc(), "id")
w_all_by_hired_on_running = Window.orderBy("hired_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_hired_on_rank_ties = Window.orderBy("hired_on_rank")
w_last_known_hired_on_month = Window.partitionBy(F.date_trunc("month", "last_known_hired_on"))
w_all_by_hired_on_rank = Window.orderBy("hired_on_rank", "id")

scoped = (
    df_support_agents
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
        F.concat(F.col("team"), F.lit("#"), F.col("id").cast("string")).alias("team_ref"),
    )
)

ranked = (
    scoped
    .select(
        "*",
        F.first("hired_on", ignorenulls=True).over(w_team_by_hired_on_running).alias("first_known_hired_on"),
        F.round(F.percent_rank().over(w_team_by_hired_on_ties), 3).alias("hired_on_percent_rank"),
        F.dense_rank().over(w_team_by_hired_on_ties_2).alias("hired_on_dense_rank"),
        F.lead("hired_on").over(w_team_by_hired_on).alias("next_hired_on"),
        F.round(F.percent_rank().over(w_all_by_hired_on_ties), 3).alias("hired_on_percent_rank_2"),
        F.rank().over(w_team_by_hired_on_ties_2).alias("hired_on_rank"),
        F.lead("hired_on", 2).over(w_team_by_hired_on).alias("next2_hired_on"),
        F.datediff(F.col("hired_on"), F.lag("hired_on").over(w_all_by_hired_on)).alias("days_since_prev_hired_on"),
        F.lead("hired_on").over(w_team_by_hired_on_2).alias("next_hired_on_2"),
        F.last("hired_on", ignorenulls=True).over(w_all_by_hired_on_running).alias("last_known_hired_on"),
    )
)

derived = (
    ranked
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
        "first_known_hired_on",
        "hired_on_percent_rank",
        "hired_on_percent_rank_2",
        "hired_on_rank",
        "next2_hired_on",
        "days_since_prev_hired_on",
        "last_known_hired_on",
        F.abs(F.col("hired_on_rank") - 5).alias("hired_on_rank_distance"),
        F.date_trunc("year", F.col("first_known_hired_on")).alias("first_known_hired_on_year"),
        F.upper(F.trim(F.col("team_ref"))).alias("team_ref_clean"),
    )
)

ranked_2 = (
    derived
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on_percent_rank",
        "hired_on_percent_rank_2",
        "hired_on_rank",
        "next2_hired_on",
        "last_known_hired_on",
        "hired_on_rank_distance",
        F.rank().over(w_all_by_hired_on_rank_ties).alias("hired_on_rank_rank"),
        F.round(F.col("hired_on_percent_rank_2") - F.avg("hired_on_percent_rank_2").over(w_last_known_hired_on_month), 2).alias("hired_on_percent_rank_2_vs_last_known_hired_on_m"),
        F.round(F.percent_rank().over(w_all_by_hired_on_rank), 3).alias("hired_on_rank_percent_rank"),
    )
)

result = (
    ranked_2
    .filter(F.col("team") == "Onboarding")
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on_percent_rank",
        "hired_on_percent_rank_2",
        "hired_on_rank",
        "hired_on_percent_rank_2_vs_last_known_hired_on_m",
        "hired_on_rank_percent_rank",
        (F.col("hired_on_percent_rank_2") + F.col("hired_on_rank")).alias("hired_on_percent_rank_2_plus_hired_on_rank"),
        F.year(F.col("next2_hired_on")).alias("next2_hired_on_year"),
    )
    .orderBy(F.col("agent_name").desc(), "id")
    .limit(10)
)
