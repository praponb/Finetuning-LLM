from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_opened_at_to_date_by = Window.partitionBy("channel", F.to_date("opened_at")).orderBy(F.col("csat").desc(), "id")
w_channel_opened_at_to_date_by_2 = Window.partitionBy("channel", F.to_date("opened_at")).orderBy(F.col("csat").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id = Window.partitionBy("agent_id")
w_opened_at_to_date_agent_id_b = Window.partitionBy(F.to_date("opened_at"), "agent_id").orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_csat = Window.partitionBy("csat")
w_opened_at_to_date_agent_id_b_2 = Window.partitionBy(F.to_date("opened_at"), "agent_id").orderBy(F.col("closed_at").desc(), "id")

base_tickets = (
    df_tickets
    .filter(F.col("requester_id") < 18)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "csat",
        "tags",
        F.round((F.unix_timestamp(F.col("closed_at")) - F.unix_timestamp(F.col("opened_at"))) / 60, 1).alias("minutes_opened_at_to_closed_at"),
        F.abs(F.col("csat") - 4).alias("csat_distance"),
        F.concat(F.col("channel"), F.lit("#"), F.col("csat").cast("string")).alias("channel_ref"),
    )
)

sequenced = (
    base_tickets
    .select(
        "*",
        F.round(F.cume_dist().over(w_channel_opened_at_to_date_by), 3).alias("csat_cume_dist"),
        F.nth_value("closed_at", 2).over(w_channel_opened_at_to_date_by_2).alias("second_closed_at"),
        F.nth_value("csat_distance", 2).over(w_channel_opened_at_to_date_by_2).alias("second_csat_distance"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_agent_id), 2).alias("requester_id_vs_agent_id_avg"),
    )
)

windowed = (
    sequenced
    .select(
        "*",
        F.first("second_closed_at").over(w_opened_at_to_date_agent_id_b).alias("first_second_closed_at"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_csat), F.lit(0)), 4).alias("csat_share_of_csat"),
        F.dense_rank().over(w_opened_at_to_date_agent_id_b_2).alias("closed_at_dense_rank"),
        F.lead("csat_cume_dist", 3).over(w_opened_at_to_date_agent_id_b_2).alias("next3_csat_cume_dist"),
        F.size(F.collect_set("agent_id").over(w_csat)).alias("distinct_agent_id_in_csat"),
    )
)

with_children = (
    windowed.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.tags",
        "s.minutes_opened_at_to_closed_at",
        "s.csat_cume_dist",
        "s.second_closed_at",
        "s.requester_id_vs_agent_id_avg",
        "s.first_second_closed_at",
        "s.closed_at_dense_rank",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        "t.event_type",
        "t.event_at",
    )
)

result = (
    with_children
    .groupBy("event_at", "event_id")
    .pivot("event_type", ["created", "escalated", "resolved"])
    .agg(F.count("requester_id_vs_agent_id_avg"))
)
