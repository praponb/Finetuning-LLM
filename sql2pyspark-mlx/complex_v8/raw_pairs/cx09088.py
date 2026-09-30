from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_hired_on_month_agent_i = Window.partitionBy(F.date_trunc("month", "agent_hired_on"), "agent_id").orderBy("least_csat_requester_id", "id").rowsBetween(-3, -1)
w_priority_agent_id = Window.partitionBy("priority", "agent_id")
w_channel = Window.partitionBy("channel")
w_agent_id_agent_tier_by_close = Window.partitionBy("agent_id", "agent_tier").orderBy(F.col("closed_at").desc_nulls_first(), "id")
w_all_by_least_csat_requester = Window.orderBy("least_csat_requester_id", "id")
w_all_by_least_csat_requester_2 = Window.orderBy("least_csat_requester_id", "id").rowsBetween(-6, -1)
w_agent_id_agent_tier_by_close_2 = Window.partitionBy("agent_id", "agent_tier").orderBy(F.col("closed_at").desc_nulls_first(), "id").rowsBetween(-6, Window.currentRow)

base_tickets = (
    df_tickets
    .filter((F.col("channel") == "email") & (F.size(F.col("tags")) >= 1))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.date_trunc("day", F.col("opened_at")).alias("opened_at_day"),
        F.least(F.col("csat"), F.col("requester_id")).alias("least_csat_requester_id"),
    )
)

combined_details = (
    base_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.closed_at",
        "s.priority",
        "s.channel",
        "s.tags",
        "s.opened_at_day",
        "s.least_csat_requester_id",
        F.col("su.hired_on").alias("agent_hired_on"),
        "su.agent_name",
        F.col("su.tier").alias("agent_tier"),
    )
)

ranked = (
    combined_details
    .select(
        "*",
        F.round(F.avg("requester_id").over(w_agent_hired_on_month_agent_i), 2).alias("requester_id_avg_3_prior"),
        F.count(F.lit(1)).over(w_priority_agent_id).alias("rows_in_priority_agent_id"),
        F.count(F.lit(1)).over(w_channel).alias("rows_in_channel"),
        F.lag("agent_hired_on").over(w_agent_id_agent_tier_by_close).alias("prev_agent_hired_on"),
        F.row_number().over(w_all_by_least_csat_requester).alias("least_csat_requester_id_seq"),
        F.max("least_csat_requester_id").over(w_all_by_least_csat_requester_2).alias("least_csat_requester_id_max_6_prior"),
        F.round(F.avg("least_csat_requester_id").over(w_agent_id_agent_tier_by_close_2), 2).alias("least_csat_requester_id_avg_7"),
    )
)

top_rows = (
    ranked
    .filter((F.col("least_csat_requester_id_seq") <= 3) & (F.col("channel").isin("phone", "web", "chat")))
)

result = (
    top_rows
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "tags",
        "opened_at_day",
        "agent_hired_on",
        "agent_name",
        "agent_tier",
        "requester_id_avg_3_prior",
        "rows_in_priority_agent_id",
        "prev_agent_hired_on",
        "least_csat_requester_id_seq",
        "least_csat_requester_id_max_6_prior",
    )
    .orderBy(F.col("agent_hired_on").desc_nulls_first(), "agent_tier")
)
