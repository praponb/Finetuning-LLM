from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_min_ticket_csat_by = Window.partitionBy("agent_id", "min_ticket_csat").orderBy("min_ticket_csat", "id")
w_agent_id_min_ticket_csat_by_2 = Window.partitionBy("agent_id", "min_ticket_csat").orderBy("min_ticket_csat", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_tickets = (
    df_tickets
    .filter(F.size(F.col("tags")) >= 1)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "csat",
        "tags",
    )
)

without_matches = (
    prepared_tickets.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.opened_at") >= F.lit("2025-02-06 11:00:00").cast("timestamp")) & (F.col("t.channel").isin("phone", "web")), "left_anti")
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.min("csat").alias("min_ticket_csat"),
        F.count("*").alias("ticket_count"),
    )
)

summarized_rows = (
    without_matches.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        "t.min_ticket_csat",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
    )
)

with_trends = (
    summarized_rows
    .select(
        "id",
        "requester_id",
        "agent_id",
        "tags",
        F.lead("min_ticket_csat").over(w_agent_id_min_ticket_csat_by).alias("next_min_ticket_csat"),
        F.nth_value("csat", 2).over(w_agent_id_min_ticket_csat_by_2).alias("second_csat"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "requester_id",
        "second_csat",
        "next_min_ticket_csat",
    )
    .unpivot(
        ids=["id"],
        values=["requester_id", "second_csat", "next_min_ticket_csat"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "field")
)
