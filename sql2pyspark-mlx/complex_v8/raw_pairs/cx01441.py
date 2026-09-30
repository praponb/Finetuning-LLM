from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority = Window.partitionBy("priority")
w_all_by_opened_at_ties = Window.orderBy("opened_at")
w_latest = Window.partitionBy("event_actor").orderBy(F.col("tags_count_vs_priority_avg").desc(), F.col("id").desc(), F.col("event_id").desc())
w_ticket_closed_at_year_closed = Window.partitionBy(F.year("ticket_closed_at"), F.year("closed_at")).orderBy("event_at", "id", "event_id")
w_all_by_csat_moving_2 = Window.orderBy(F.col("csat").desc(), "id", "event_id").rowsBetween(-2, Window.currentRow)
w_event_event_type_by_closed_a = Window.partitionBy("event_event_type").orderBy("closed_at", "id", "event_id").rowsBetween(-1, -1)
w_all_by_csat_moving_3 = Window.orderBy(F.col("csat").desc(), "id", "event_id").rowsBetween(-3, Window.currentRow)

base = (
    df_tickets
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
        F.substring(F.trim(F.col("priority")), 1, 4).alias("priority_prefix"),
        F.size(F.col("tags")).alias("tags_count"),
        F.when(F.col("channel").isin("chat", "phone"), "group_a").when(F.col("channel") == "web", "group_b").otherwise("other").alias("channel_group"),
    )
)

sequenced = (
    base
    .select(
        "id",
        "requester_id",
        "opened_at",
        "closed_at",
        "csat",
        "priority_prefix",
        "tags_count",
        "channel_group",
        F.sum("csat").over(w_priority).alias("priority_sum_csat"),
        F.round(F.col("tags_count") - F.avg("tags_count").over(w_priority), 2).alias("tags_count_vs_priority_avg"),
        F.rank().over(w_all_by_opened_at_ties).alias("opened_at_rank"),
    )
)

detail_rows = (
    sequenced.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.opened_at",
        "s.closed_at",
        "s.csat",
        "s.priority_prefix",
        "s.priority_sum_csat",
        "s.tags_count_vs_priority_avg",
        "s.opened_at_rank",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        "t.event_at",
        "t.event_type",
    )
)

enriched = (
    detail_rows.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.id") == F.col("s.event_id"), "left")
    .join(df_tickets.alias("ti"), F.col("ti.id") == F.col("t.ticket_id"), "left")
    .select(
        "s.id",
        "s.closed_at",
        "s.csat",
        "s.priority_prefix",
        "s.priority_sum_csat",
        "s.tags_count_vs_priority_avg",
        "s.event_id",
        "s.event_actor",
        "s.event_at",
        "s.event_type",
        F.col("t.ticket_id").alias("event_ticket_id"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.actor").alias("event_actor_2"),
        F.col("ti.closed_at").alias("ticket_closed_at"),
    )
)

deduped = (
    enriched
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_trends = (
    deduped
    .select(
        "*",
        F.lag("event_at").over(w_ticket_closed_at_year_closed).alias("prev_event_at"),
        F.max("csat").over(w_all_by_csat_moving_2).alias("csat_max_3"),
        F.max("tags_count_vs_priority_avg").over(w_event_event_type_by_closed_a).alias("tags_count_vs_priority_avg_max_1_prior"),
        F.round(F.avg("tags_count_vs_priority_avg").over(w_all_by_csat_moving_3), 2).alias("tags_count_vs_priority_avg_avg_4"),
    )
)

result = (
    with_trends
    .groupingSets(
        [["event_actor", "event_event_type"], ["event_actor"], ["event_event_type"]],
        "event_actor", "event_event_type",
    )
    .agg(F.count("*").alias("n_rows"))
    .orderBy(
        F.col("event_actor").asc_nulls_last(),
        F.col("event_event_type").asc_nulls_last(),
    )
)
