from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("priority", "agent_id").orderBy(F.col("requester_id").desc(), F.col("id").desc())
w_agent_id_event_count = Window.partitionBy("agent_id", "event_count")
w_channel_by_first_event_at_2 = Window.partitionBy("channel").orderBy(F.col("first_event_at_2").desc(), "id").rowsBetween(-3, Window.currentRow)

filtered_tickets = (
    df_tickets
    .filter(F.col("priority").isin("P2", "P4", "P3"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "channel",
        "csat",
        "tags",
        F.round((F.unix_timestamp(F.col("closed_at")) - F.unix_timestamp(F.col("opened_at"))) / 60, 1).alias("minutes_opened_at_to_closed_at"),
    )
)

deduped = (
    filtered_tickets
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count("*").alias("event_count"),
        F.min("event_at").alias("first_event_at"),
        F.min("event_at").alias("first_event_at_2"),
    )
)

profiled = (
    deduped.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        "t.first_event_at",
        "t.first_event_at_2",
    )
)

with_trends = (
    profiled
    .select(
        "id",
        "agent_id",
        "priority",
        "channel",
        "csat",
        "tags",
        "minutes_opened_at_to_closed_at",
        "event_count",
        F.sum("csat").over(w_agent_id_event_count).alias("agent_id_event_count_sum_csat"),
        F.round(F.col("csat") - F.avg("csat").over(w_agent_id_event_count), 2).alias("csat_vs_agent_id_event_count_avg"),
        F.count(F.lit(1)).over(w_agent_id_event_count).alias("rows_in_agent_id_event_count"),
        F.round(F.avg("csat").over(w_channel_by_first_event_at_2), 2).alias("csat_avg_4"),
    )
)

result = (
    with_trends
    .select("id", "agent_id_event_count_sum_csat", "event_count")
    .unpivot(
        ids=["id"],
        values=["agent_id_event_count_sum_csat", "event_count"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "field")
)
