from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_closed_at_to_date = Window.partitionBy("priority", F.to_date("closed_at"))
w_priority_by_closed_at = Window.partitionBy("priority").orderBy("closed_at", "id")
w_priority_by_closed_at_runnin = Window.partitionBy("priority").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_tickets
    .filter((F.col("csat") <= 4) & (F.col("agent_id").isNotNull()))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "tags",
    )
)

windowed = (
    cleaned
    .select(
        "*",
        F.size(F.collect_set("agent_id").over(w_priority_closed_at_to_date)).alias("distinct_agent_id_in_priority_closed_at_to_date"),
        F.rank().over(w_priority_by_closed_at).alias("closed_at_rank"),
        F.last("opened_at", ignorenulls=True).over(w_priority_by_closed_at_runnin).alias("last_known_opened_at"),
        F.round(F.avg("requester_id").over(w_priority_closed_at_to_date), 2).alias("priority_closed_at_to_date_avg_requester_id"),
    )
)

agent_id_stats = (
    windowed
    .groupBy("agent_id")
    .agg(F.avg("requester_id").alias("avg_requester_id"))
)

above_average = (
    windowed.alias("s")
    .join(agent_id_stats.alias("g"), F.col("g.agent_id") == F.col("s.agent_id"), "inner")
    .filter(F.col("s.requester_id") > F.col("g.avg_requester_id"))
    .select(
        "s.*",
        "g.avg_requester_id",
        F.round(F.col("s.requester_id") - F.col("g.avg_requester_id"), 2).alias("requester_id_minus_avg"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.min("closed_at").alias("first_ticket_closed_at"),
        F.min("closed_at").alias("first_ticket_closed_at_2"),
    )
)

profiled = (
    above_average.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select("s.*", "t.first_ticket_closed_at", "t.first_ticket_closed_at_2")
)

combined_details_agent = (
    profiled.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        "su.agent_name",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.team").alias("agent_team"),
    )
)

combined_details_agent_2 = (
    combined_details_agent.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.*",
        F.col("su.agent_name").alias("agent_agent_name"),
        F.col("su.tier").alias("agent_tier"),
    )
)

totals = (
    combined_details_agent_2
    .filter(F.col("requester_id_minus_avg").between(-17.5, 10.0))
    .groupBy(
        F.to_date("last_known_opened_at").alias("last_known_opened_at_day"),
        "distinct_agent_id_in_priority_closed_at_to_date",
        F.date_trunc("month", "first_ticket_closed_at_2").alias("first_ticket_closed_at_2_month"),
    )
    .agg(
        F.round(F.sum("requester_id_minus_avg"), 2).alias("total_requester_id_minus_avg"),
        F.round(F.sum("avg_requester_id") / F.nullif(F.sum("distinct_agent_id_in_priority_closed_at_to_date"), F.lit(0)), 3).alias("avg_requester_id_to_distinct_agent_id_in_priorit"),
        F.round(F.sum("requester_id") / F.nullif(F.sum("priority_closed_at_to_date_avg_requester_id"), F.lit(0)), 3).alias("requester_id_to_priority_closed_at_to_date_avg_r"),
    )
)

result = (
    totals
    .select(
        "last_known_opened_at_day",
        "distinct_agent_id_in_priority_closed_at_to_date",
        "total_requester_id_minus_avg",
        "requester_id_to_priority_closed_at_to_date_avg_r",
        F.date_format(F.col("last_known_opened_at_day"), "yyyy-MM-dd").alias("last_known_opened_at_day_label"),
        F.quarter(F.col("last_known_opened_at_day")).alias("last_known_opened_at_day_quarter"),
    )
    .orderBy(
        F.col("requester_id_to_priority_closed_at_to_date_avg_r").asc(),
        "distinct_agent_id_in_priority_closed_at_to_date",
    )
)
