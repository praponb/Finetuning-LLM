from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_agent_team_by_agent = Window.partitionBy("priority", "agent_team").orderBy(F.col("agent_hired_on").desc())
w_priority_agent_team_by_agent_2 = Window.partitionBy("priority", "agent_team").orderBy(F.col("agent_hired_on").desc(), "id")
w_all_by_agent_hired_on = Window.orderBy("agent_hired_on", "id")
w_all_by_agent_hired_on_ties = Window.orderBy("agent_hired_on")
w_all_by_agent_hired_on_runnin = Window.orderBy("agent_hired_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_opened_at_year_agent_id_by_o = Window.partitionBy(F.year("opened_at"), "agent_id").orderBy(F.col("opened_at").asc_nulls_last(), "id")
w_agent_hired_on_rank = Window.partitionBy("agent_hired_on_rank")
w_priority_by_agent_hired_on_r = Window.partitionBy("priority").orderBy("agent_hired_on_rank", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_hired_on_rank_priority = Window.partitionBy("agent_hired_on_rank", "priority").orderBy("opened_at_quintile", "id").rowsBetween(-6, Window.currentRow)
w_agent_hired_on_rank_priority_2 = Window.partitionBy("agent_hired_on_rank", "priority").orderBy("opened_at_quintile", "id")
w_first_known_opened_at_month = Window.partitionBy(F.month("first_known_opened_at"), "agent_hired_on_rank_2").orderBy(F.col("next_opened_at").desc())

recent = (
    df_tickets
    .filter(F.col("opened_at") >= F.lit("2025-01-31 08:00:00").cast("timestamp"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "tags",
    )
)

joined = (
    recent.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.tier").isNotNull()), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.priority",
        "s.tags",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

windowed = (
    joined
    .select(
        "*",
        F.rank().over(w_priority_agent_team_by_agent).alias("agent_hired_on_rank"),
        F.round(F.cume_dist().over(w_priority_agent_team_by_agent_2), 3).alias("agent_hired_on_cume_dist"),
        F.row_number().over(w_all_by_agent_hired_on).alias("seq_no"),
        F.rank().over(w_all_by_agent_hired_on_ties).alias("agent_hired_on_rank_2"),
        F.lead("opened_at").over(w_priority_agent_team_by_agent_2).alias("next_opened_at"),
        F.first("opened_at", ignorenulls=True).over(w_all_by_agent_hired_on_runnin).alias("first_known_opened_at"),
        F.datediff(F.col("agent_hired_on"), F.lag("agent_hired_on").over(w_all_by_agent_hired_on)).alias("days_since_prev_agent_hired_on"),
        F.ntile(5).over(w_opened_at_year_agent_id_by_o).alias("opened_at_quintile"),
        F.last("opened_at", ignorenulls=True).over(w_all_by_agent_hired_on_runnin).alias("last_known_opened_at"),
        F.ntile(10).over(w_opened_at_year_agent_id_by_o).alias("opened_at_decile"),
        F.ntile(10).over(w_priority_agent_team_by_agent_2).alias("agent_hired_on_decile"),
        F.rank().over(w_opened_at_year_agent_id_by_o).alias("opened_at_rank"),
    )
)

with_windows = (
    windowed
    .select(
        "*",
        F.round(F.col("agent_hired_on_rank_2") - F.avg("agent_hired_on_rank_2").over(w_agent_hired_on_rank), 2).alias("agent_hired_on_rank_2_vs_agent_hired_on_rank_avg"),
        F.last("opened_at_quintile").over(w_priority_by_agent_hired_on_r).alias("last_opened_at_quintile"),
        F.max("agent_hired_on_cume_dist").over(w_agent_hired_on_rank_priority).alias("agent_hired_on_cume_dist_max_7"),
        (F.col("opened_at_quintile") - F.lag("opened_at_quintile").over(w_agent_hired_on_rank_priority_2)).alias("opened_at_quintile_change"),
        F.rank().over(w_first_known_opened_at_month).alias("next_opened_at_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "tags",
        "agent_hired_on",
        "agent_hired_on_rank",
        "days_since_prev_agent_hired_on",
        "opened_at_quintile",
        "last_known_opened_at",
        "opened_at_decile",
        "opened_at_rank",
        "last_opened_at_quintile",
        "agent_hired_on_cume_dist_max_7",
        "opened_at_quintile_change",
    )
    .orderBy(F.col("priority").asc())
)
