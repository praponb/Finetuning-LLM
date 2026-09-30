from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_opened_at_to_date = Window.partitionBy(F.to_date("opened_at"))
w_all_by_priority_len = Window.orderBy("priority_len", "id")

cleaned = (
    df_tickets
    .filter(F.hour(F.col("closed_at")).between(10, 15))
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
        F.round((F.unix_timestamp(F.col("closed_at")) - F.unix_timestamp(F.col("opened_at"))) / 60, 1).alias("minutes_opened_at_to_closed_at"),
        F.length(F.trim(F.col("priority"))).alias("priority_len"),
    )
)

windowed = (
    cleaned
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        "priority_len",
        F.count(F.lit(1)).over(w_opened_at_to_date).alias("rows_in_opened_at_to_date"),
        F.size(F.collect_set("channel").over(w_opened_at_to_date)).alias("distinct_channel_in_opened_at_to_date"),
        F.round((F.unix_timestamp("closed_at") - F.unix_timestamp(F.lag("closed_at").over(w_all_by_priority_len))) / 60, 1).alias("minutes_since_prev_closed_at"),
        F.lag("csat", 2).over(w_all_by_priority_len).alias("prev2_csat"),
        (F.col("csat") - F.lag("csat").over(w_all_by_priority_len)).alias("csat_change"),
    )
)

without_matches = (
    windowed.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left_semi")
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count(F.when(F.col("priority") == "P1", 1)).alias("ticket_priority_count"),
    )
)

profiled = (
    without_matches.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_priority_count"), F.lit(0)).alias("ticket_priority_count"),
    )
)

result = (
    profiled
    .groupingSets(
        [["priority", "priority_len"], ["priority"]],
        "priority", "priority_len",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("priority").isin("P1", "P3"), F.col("csat_change")).otherwise(0)).alias("csat_change_priority_in_p1_p3"),
    )
)
