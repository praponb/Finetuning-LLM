from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_group = Window.partitionBy("event_type_group")
w_all_by_first_event_at = Window.orderBy(F.col("first_event_at").desc(), "id")

valid_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("event_type").isin("reopened", "created"), "group_a").when(F.col("event_type") == "assigned", "group_b").otherwise("other").alias("event_type_group"),
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
        F.substring(F.trim(F.col("actor")), 1, 2).alias("actor_prefix"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count("*").alias("event_count"),
        F.min("event_at").alias("first_event_at"),
        F.count("*").alias("event_count_2"),
    )
)

with_counts = (
    valid_events.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        "t.first_event_at",
        F.coalesce(F.col("t.event_count_2"), F.lit(0)).alias("event_count_2"),
    )
)

event_stats_2 = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count(F.when(F.col("event_type") != "resolved", 1)).alias("event_event_type_count"),
        F.max("event_at").alias("last_event_at"),
        F.min("event_at").alias("first_event_at_2"),
    )
)

with_counts_2 = (
    with_counts.alias("s")
    .join(event_stats_2.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_event_type_count"), F.lit(0)).alias("event_event_type_count"),
        "t.last_event_at",
        "t.first_event_at_2",
    )
)

metrics = (
    with_counts_2
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "event_type_group",
        "event_type_len",
        "actor_prefix",
        "first_event_at",
        "event_count_2",
        "event_event_type_count",
        "last_event_at",
        "first_event_at_2",
        F.count(F.lit(1)).over(w_event_type_group).alias("rows_in_event_type_group"),
        F.row_number().over(w_all_by_first_event_at).alias("seq_no"),
        F.min("event_count").over(w_event_type_group).alias("event_type_group_min_event_count"),
    )
)

result = (
    metrics
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "event_type_group",
        "event_type_len",
        "first_event_at",
        "event_count_2",
        "event_event_type_count",
        "last_event_at",
        "first_event_at_2",
        "rows_in_event_type_group",
        "seq_no",
        "event_type_group_min_event_count",
    )
    .orderBy(
        F.col("event_count_2").desc(),
        F.col("event_type_group").desc_nulls_first(),
    )
)
