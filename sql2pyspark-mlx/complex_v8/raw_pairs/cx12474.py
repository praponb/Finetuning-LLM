from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel = Window.partitionBy("channel")
w_priority_agent_id = Window.partitionBy("priority", "agent_id")
w_priority_channel_by_closed_a = Window.partitionBy("priority", "channel").orderBy("closed_at", "id")
w_agent_id_closed_at_to_date = Window.partitionBy("agent_id", F.to_date("closed_at"))
w_agent_team_by_closed_at_full = Window.partitionBy("agent_team").orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_team_agent_id_by_agent = Window.partitionBy("agent_team", "agent_id").orderBy("agent_hired_on")
w_opened_at_to_date_by_opened = Window.partitionBy(F.to_date("opened_at")).orderBy(F.col("opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_opened_at_to_date_by_opened_2 = Window.partitionBy(F.to_date("opened_at")).orderBy(F.col("opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_opened_at_to_date_by_opened_3 = Window.partitionBy(F.to_date("opened_at")).orderBy(F.col("opened_at").desc(), "id")
w_second_opened_at_month_by_ne = Window.partitionBy(F.date_trunc("month", "second_opened_at")).orderBy(F.col("next_requester_id").desc(), "id")
w_agent_hired_on_month = Window.partitionBy(F.date_trunc("month", "agent_hired_on"))
w_opened_at_month_agent_hired = Window.partitionBy(F.month("opened_at"), F.year("agent_hired_on")).orderBy(F.col("agent_id_closed_at_to_date_avg_requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_opened_at_month_agent_hired_2 = Window.partitionBy(F.month("opened_at"), F.year("agent_hired_on")).orderBy(F.col("agent_id_closed_at_to_date_avg_requester_id").desc())
w_agent_id_closed_at_decile = Window.partitionBy("agent_id", "closed_at_decile")
w_all_by_opened_at_full = Window.orderBy(F.col("opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_tickets = (
    df_tickets
    .filter((F.col("requester_id") >= 9) | (F.col("channel") == "email"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
    )
)

with_parent = (
    filtered_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.upper(F.trim(F.col("su.agent_name"))).like("L%"))
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.channel",
        "s.tags",
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
        "su.agent_name",
    )
)

with_windows = (
    with_parent
    .select(
        "*",
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_channel), 2).alias("requester_id_vs_channel_avg"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_priority_agent_id), F.lit(0)), 4).alias("requester_id_share_of_priority_agent_id"),
        F.ntile(10).over(w_priority_channel_by_closed_a).alias("closed_at_decile"),
        F.count(F.lit(1)).over(w_agent_id_closed_at_to_date).alias("rows_in_agent_id_closed_at_to_date"),
        F.nth_value("opened_at", 2).over(w_agent_team_by_closed_at_full).alias("second_opened_at"),
        F.round(F.cume_dist().over(w_agent_team_agent_id_by_agent), 3).alias("agent_hired_on_cume_dist"),
        F.nth_value("closed_at", 2).over(w_opened_at_to_date_by_opened).alias("second_closed_at"),
        F.min("requester_id").over(w_opened_at_to_date_by_opened_2).alias("min_requester_id_so_far"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_channel), F.lit(0)), 4).alias("requester_id_share_of_channel"),
        F.count(F.lit(1)).over(w_priority_agent_id).alias("rows_in_priority_agent_id"),
        F.lead("requester_id", 1, 0).over(w_opened_at_to_date_by_opened_3).alias("next_requester_id"),
        F.round(F.avg("requester_id").over(w_agent_id_closed_at_to_date), 2).alias("agent_id_closed_at_to_date_avg_requester_id"),
    )
)

windowed = (
    with_windows
    .select(
        "id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "agent_team",
        "agent_hired_on",
        "requester_id_vs_channel_avg",
        "requester_id_share_of_priority_agent_id",
        "rows_in_agent_id_closed_at_to_date",
        "agent_hired_on_cume_dist",
        "second_closed_at",
        "requester_id_share_of_channel",
        "rows_in_priority_agent_id",
        "next_requester_id",
        "agent_id_closed_at_to_date_avg_requester_id",
        F.round(F.cume_dist().over(w_second_opened_at_month_by_ne), 3).alias("next_requester_id_cume_dist"),
        F.size(F.collect_set("channel").over(w_agent_hired_on_month)).alias("distinct_channel_in_agent_hired_on_month"),
        F.last("requester_id").over(w_opened_at_month_agent_hired).alias("last_requester_id"),
        F.round(F.percent_rank().over(w_opened_at_month_agent_hired_2), 3).alias("agent_id_closed_at_to_date_avg_requester_id_perc"),
        F.round(F.col("rows_in_priority_agent_id") - F.avg("rows_in_priority_agent_id").over(w_agent_id_closed_at_decile), 2).alias("rows_in_priority_agent_id_vs_agent_id_closed_at_"),
        F.nth_value("second_opened_at", 2).over(w_all_by_opened_at_full).alias("second_second_opened_at"),
    )
)

result = (
    windowed
    .filter(F.col("next_requester_id") >= 8)
    .select(
        "id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "agent_team",
        "agent_hired_on",
        "requester_id_vs_channel_avg",
        "requester_id_share_of_priority_agent_id",
        "rows_in_agent_id_closed_at_to_date",
        "agent_hired_on_cume_dist",
        "requester_id_share_of_channel",
        "rows_in_priority_agent_id",
        "next_requester_id",
        "next_requester_id_cume_dist",
        "distinct_channel_in_agent_hired_on_month",
        "last_requester_id",
        "rows_in_priority_agent_id_vs_agent_id_closed_at_",
        "second_second_opened_at",
        F.length(F.trim(F.col("channel"))).alias("channel_len"),
    )
    .orderBy(F.col("requester_id_share_of_channel").desc(), "id")
    .limit(10)
)
