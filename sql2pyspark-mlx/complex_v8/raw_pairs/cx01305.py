from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id = Window.partitionBy("ticket_id")
w_actor_by_last_event_at_full = Window.partitionBy("actor").orderBy("last_event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_len = Window.partitionBy("actor_len")
w_actor_by_last_event_at_runni = Window.partitionBy("actor").orderBy("last_event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_len_event_at_month_by = Window.partitionBy("actor_len", F.date_trunc("month", "event_at")).orderBy(F.col("ticket_id_min_event_count").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_events = (
    df_ticket_events
    .filter((F.col("event_type") == "assigned") | (F.col("ticket_id").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.count("*").alias("event_count"),
    )
)

summarized_rows = (
    base_events.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.last_event_at",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

with_activity = (
    summarized_rows.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.opened_at") >= F.lit("2025-02-06 11:00:00").cast("timestamp")) & (F.col("t.csat") > 2), "left_anti")
)

ranked = (
    with_activity
    .select(
        "id",
        "event_at",
        "actor",
        "last_event_at",
        F.sum("event_count").over(w_ticket_id).alias("ticket_id_sum_event_count"),
        F.min("event_count").over(w_ticket_id).alias("ticket_id_min_event_count"),
        F.round(F.col("event_count") - F.avg("event_count").over(w_ticket_id), 2).alias("event_count_vs_ticket_id_avg"),
    )
)

enriched = (
    ranked
    .select(
        "*",
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
        F.date_format(F.col("last_event_at"), "MMM yyyy").alias("last_event_at_label"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.first("last_event_at").over(w_actor_by_last_event_at_full).alias("first_last_event_at"),
        F.count(F.lit(1)).over(w_actor_len).alias("rows_in_actor_len"),
        F.max("ticket_id_min_event_count").over(w_actor_len).alias("actor_len_max_ticket_id_min_event_count"),
        F.max("event_count_vs_ticket_id_avg").over(w_actor_by_last_event_at_runni).alias("max_event_count_vs_ticket_id_avg_so_far"),
        F.max("ticket_id_sum_event_count").over(w_actor_len_event_at_month_by).alias("max_ticket_id_sum_event_count_so_far"),
    )
)

enriched_2 = (
    with_windows
    .filter(F.col("actor_len_max_ticket_id_min_event_count") < 720)
    .select(
        "id",
        "actor",
        "last_event_at",
        "ticket_id_sum_event_count",
        "ticket_id_min_event_count",
        "event_count_vs_ticket_id_avg",
        "actor_len",
        "last_event_at_label",
        "first_last_event_at",
        "rows_in_actor_len",
        "actor_len_max_ticket_id_min_event_count",
        "max_event_count_vs_ticket_id_avg_so_far",
        F.round(F.col("rows_in_actor_len") / F.nullif(F.col("actor_len"), F.lit(0)), 3).alias("rows_in_actor_len_per_actor_len"),
        F.date_trunc("week", F.col("event_at")).alias("event_at_week"),
    )
)

result = (
    enriched_2
    .select(
        "id",
        "ticket_id_sum_event_count",
        "ticket_id_min_event_count",
        "actor_len",
        "first_last_event_at",
        "rows_in_actor_len",
        "max_event_count_vs_ticket_id_avg_so_far",
        "rows_in_actor_len_per_actor_len",
        "event_at_week",
        F.month(F.col("last_event_at")).alias("last_event_at_month"),
        F.date_trunc("hour", F.col("last_event_at")).alias("last_event_at_hour"),
    )
)
