from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_agent_team = Window.partitionBy("agent_id", "agent_team")
w_priority_by_closed_at = Window.partitionBy("priority").orderBy("closed_at", "id")
w_agent_team_by_closed_at = Window.partitionBy("agent_team").orderBy(F.col("closed_at").asc_nulls_last(), "id")
w_priority_by_closed_at_full = Window.partitionBy("priority").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_opened_at_month_agent_id = Window.partitionBy(F.date_trunc("month", "opened_at"), "agent_id")
w_all_by_closed_at = Window.orderBy("closed_at", "id")
w_agent_team_by_closed_at_ties = Window.partitionBy("agent_team").orderBy(F.col("closed_at").asc_nulls_last())
w_all_by_closed_at_2 = Window.orderBy(F.col("closed_at").desc(), "id")

valid = (
    df_tickets
    .filter(F.col("priority") != "P3")
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        F.length(F.trim(F.col("channel"))).alias("channel_len"),
    )
)

combined_details_agent = (
    valid.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.col("su.team").isin("VIP", "Onboarding", "Tech"))
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.csat",
        F.col("su.team").alias("agent_team"),
    )
)

sequenced = (
    combined_details_agent
    .select(
        "id",
        "opened_at",
        "agent_team",
        F.min("csat").over(w_agent_id_agent_team).alias("agent_id_agent_team_min_csat"),
        F.round(F.percent_rank().over(w_priority_by_closed_at), 3).alias("closed_at_percent_rank"),
        F.round(F.col("csat") - F.avg("csat").over(w_agent_id_agent_team), 2).alias("csat_vs_agent_id_agent_team_avg"),
        F.ntile(3).over(w_agent_team_by_closed_at).alias("closed_at_tercile"),
        F.first("csat").over(w_priority_by_closed_at_full).alias("first_csat"),
        F.size(F.collect_set("agent_team").over(w_opened_at_month_agent_id)).alias("distinct_agent_team_in_opened_at_month_agent_id"),
        F.nth_value("closed_at", 2).over(w_priority_by_closed_at_full).alias("second_closed_at"),
        F.lead("csat").over(w_agent_team_by_closed_at).alias("next_csat"),
        F.count(F.lit(1)).over(w_agent_id_agent_team).alias("rows_in_agent_id_agent_team"),
        F.row_number().over(w_all_by_closed_at).alias("seq_no"),
        F.rank().over(w_agent_team_by_closed_at_ties).alias("closed_at_rank"),
        F.lead("csat", 3).over(w_all_by_closed_at_2).alias("next3_csat"),
        F.count(F.lit(1)).over(w_opened_at_month_agent_id).alias("rows_in_opened_at_month_agent_id"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "opened_at",
        "agent_id_agent_team_min_csat",
        "closed_at_percent_rank",
        "first_csat",
        "second_closed_at",
        "next_csat",
        "rows_in_agent_id_agent_team",
        "seq_no",
        "rows_in_opened_at_month_agent_id",
        F.abs(F.col("closed_at_percent_rank") - 0.7).alias("closed_at_percent_rank_distance"),
        F.coalesce(F.col("agent_team"), F.lit("UNKNOWN")).alias("agent_team_or_unknown"),
    )
    .orderBy(F.col("id").desc(), F.col("closed_at_percent_rank").desc())
    .limit(5)
)
