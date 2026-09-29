from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_actor_csat = Window.partitionBy("event_actor", "csat")
w_all_by_csat_running = Window.orderBy("csat", "id", "event_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_event_at = Window.orderBy("event_at", "id", "event_id")
w_all_by_event_at_prior_1 = Window.orderBy("event_at", "id", "event_id").rowsBetween(-1, -1)

valid_tickets = (
    df_tickets
    .filter(F.col("requester_id") <= 22)
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
        F.array_join(F.array_sort(F.col("tags")), ",").alias("tags_text"),
    )
)

enriched = (
    valid_tickets
    .select(
        "*",
        F.concat_ws("|", F.col("tags_text"), F.col("priority")).alias("tags_text_priority_label"),
        F.date_format(F.col("closed_at"), "yyyy-MM").alias("closed_at_label"),
    )
)

enriched_2 = (
    enriched
    .select(
        "*",
        F.coalesce(F.col("csat"), F.lit(0)).alias("csat_filled"),
        F.round(F.col("requester_id") / F.nullif(F.col("csat"), F.lit(0)), 3).alias("requester_id_per_csat"),
    )
)

line_level = (
    enriched_2.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.channel",
        "s.csat",
        "s.tags_text",
        "s.closed_at_label",
        "s.csat_filled",
        "s.requester_id_per_csat",
        F.col("t.id").alias("event_id"),
        "t.event_at",
        F.col("t.actor").alias("event_actor"),
    )
)

with_trends = (
    line_level
    .select(
        "id",
        "agent_id",
        "channel",
        "tags_text",
        "closed_at_label",
        "csat_filled",
        "requester_id_per_csat",
        "event_id",
        "event_at",
        F.round(F.avg("csat").over(w_event_actor_csat), 2).alias("event_actor_csat_avg_csat"),
        F.last("csat_filled", ignorenulls=True).over(w_all_by_csat_running).alias("last_known_csat_filled"),
        F.sum("requester_id_per_csat").over(w_all_by_csat_running).alias("running_requester_id_per_csat"),
        F.row_number().over(w_all_by_event_at).alias("rn"),
        F.lag("event_at").over(w_all_by_event_at).alias("prev_event_at"),
        F.sum("csat_filled").over(w_all_by_event_at_prior_1).alias("csat_filled_sum_1_prior"),
    )
)

result = (
    with_trends
    .groupBy("last_known_csat_filled")
    .pivot("channel", ["chat", "phone"])
    .agg(F.sum("csat_filled"))
)
