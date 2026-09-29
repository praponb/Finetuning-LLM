from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_opened_at_to_date = Window.partitionBy("priority", F.to_date("opened_at"))
w_priority_by_csat_running = Window.partitionBy("priority").orderBy(F.col("csat").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_priority_csat_by_agent_hired = Window.partitionBy("priority", "csat").orderBy(F.col("agent_hired_on").desc(), "id")
w_csat_by_closed_at = Window.partitionBy("csat").orderBy("closed_at", "id")
w_priority_by_csat = Window.partitionBy("priority").orderBy(F.col("csat").desc(), "id")

recent = (
    df_tickets
    .filter(F.col("csat").between(1, 5))
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

enriched = (
    recent.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.year(F.col("su.hired_on")) == 2024)
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.channel",
        "s.csat",
        "s.tags",
        F.col("su.hired_on").alias("agent_hired_on"),
        "su.agent_name",
        F.col("su.tier").alias("agent_tier"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_priority_opened_at_to_date), 2).alias("requester_id_vs_priority_opened_at_to_date_avg"),
        F.last("closed_at", ignorenulls=True).over(w_priority_by_csat_running).alias("last_known_closed_at"),
        F.rank().over(w_priority_csat_by_agent_hired).alias("agent_hired_on_rank"),
        F.max("requester_id").over(w_priority_opened_at_to_date).alias("priority_opened_at_to_date_max_requester_id"),
        F.round(F.col("csat") - F.avg("csat").over(w_priority_opened_at_to_date), 2).alias("csat_vs_priority_opened_at_to_date_avg"),
        F.lag("agent_hired_on").over(w_priority_csat_by_agent_hired).alias("prev_agent_hired_on"),
        F.row_number().over(w_csat_by_closed_at).alias("seq_no"),
        F.row_number().over(w_priority_csat_by_agent_hired).alias("agent_hired_on_seq"),
        F.row_number().over(w_priority_by_csat).alias("csat_seq"),
        (F.col("csat") - F.lag("csat").over(w_priority_csat_by_agent_hired)).alias("csat_change"),
        F.size(F.collect_set("agent_id").over(w_priority_opened_at_to_date)).alias("distinct_agent_id_in_priority_opened_at_to_date"),
    )
)

result = (
    with_windows
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
        "agent_hired_on",
        "agent_tier",
        "requester_id_vs_priority_opened_at_to_date_avg",
        "agent_hired_on_rank",
        "priority_opened_at_to_date_max_requester_id",
        "agent_hired_on_seq",
        "csat_seq",
        "distinct_agent_id_in_priority_opened_at_to_date",
    )
    .orderBy(F.col("agent_tier").asc(), "id")
    .limit(20)
)
