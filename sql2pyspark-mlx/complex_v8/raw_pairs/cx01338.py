from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_len_by_channel_len_r = Window.partitionBy("channel_len").orderBy("channel_len", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_csat = Window.partitionBy("csat")
w_all_by_channel_len_ties = Window.orderBy("channel_len")

base = (
    df_tickets
    .filter((F.col("priority").isin("P1", "P3", "P4")) & (F.col("requester_id") > 7))
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
        F.length(F.trim(F.col("channel"))).alias("channel_len"),
        F.date_format(F.col("opened_at"), "yyyy-MM-dd HH:00").alias("opened_at_label"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count(F.when(F.col("actor") == "agent", 1)).alias("event_actor_count"),
        F.max("event_at").alias("last_event_at"),
    )
)

profiled = (
    base.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_actor_count"), F.lit(0)).alias("event_actor_count"),
        "t.last_event_at",
    )
)

sequenced = (
    profiled
    .select(
        "id",
        "requester_id",
        "priority",
        "channel",
        "csat",
        "tags",
        "channel_len",
        "event_actor_count",
        "last_event_at",
        F.count(F.lit(1)).over(w_channel_len_by_channel_len_r).alias("running_count"),
        F.min("csat").over(w_csat).alias("csat_min_csat"),
        F.min("requester_id").over(w_csat).alias("csat_min_requester_id"),
        F.min("channel_len").over(w_channel_len_by_channel_len_r).alias("min_channel_len_so_far"),
        F.rank().over(w_all_by_channel_len_ties).alias("channel_len_rank"),
        F.round(F.percent_rank().over(w_all_by_channel_len_ties), 3).alias("channel_len_percent_rank"),
    )
)

result = (
    sequenced
    .select(
        "id",
        F.col("channel_len").alias("channel_len_v"),
        F.col("csat_min_csat").alias("csat_min_csat_v"),
        F.col("min_channel_len_so_far").alias("m_min_channel_len_so_far"),
        F.col("event_actor_count").alias("m_event_actor_count"),
    )
    .unpivot(
        ids=["id"],
        values=["channel_len_v", "csat_min_csat_v", "m_min_channel_len_so_far", "m_event_actor_count"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
