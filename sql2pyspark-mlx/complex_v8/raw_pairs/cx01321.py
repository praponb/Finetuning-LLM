from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_id_event_event_type = Window.partitionBy("event_id", "event_event_type")
w_event_type_by_event_at_prior = Window.partitionBy("event_type").orderBy("event_at", "ticket_id", "event_type", "event_at_ym", "event_id").rowsBetween(-3, -1)
w_event_type_by_event_at = Window.partitionBy("event_type").orderBy("event_at", "ticket_id", "event_type", "event_at_ym", "event_id")
w_event_id_ticket_id_by_total = Window.partitionBy("event_id", "ticket_id").orderBy("total_days_since_event_at", "ticket_id", "event_type", "event_at_ym", "event_id")

cleaned = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

totals = (
    cleaned
    .filter(F.col("event_at") < F.lit("2025-03-02 09:00:00").cast("timestamp"))
    .groupBy(
        "ticket_id",
        "event_type",
        F.date_format("event_at", "yyyy-MM").alias("event_at_ym"),
    )
    .agg(
        F.array_sort(F.collect_set("actor")).alias("actor_list"),
        F.sum("days_since_event_at").alias("total_days_since_event_at"),
        F.max("days_since_event_at").alias("max_days_since_event_at"),
    )
)

detail_rows_events = (
    totals.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.ticket_id",
        "s.event_type",
        "s.event_at_ym",
        "s.total_days_since_event_at",
        F.col("t.id").alias("event_id"),
        "t.event_at",
        F.col("t.event_type").alias("event_event_type"),
    )
)

with_windows = (
    detail_rows_events
    .select(
        "*",
        F.size(F.collect_set("ticket_id").over(w_event_id_event_event_type)).alias("distinct_ticket_id_in_event_id_event_event_type"),
        F.max("total_days_since_event_at").over(w_event_type_by_event_at_prior).alias("total_days_since_event_at_max_3_prior"),
        F.size(F.collect_set("event_type").over(w_event_id_event_event_type)).alias("distinct_event_type_in_event_id_event_event_type"),
        F.row_number().over(w_event_type_by_event_at).alias("event_at_seq"),
        (F.col("total_days_since_event_at") - F.lag("total_days_since_event_at").over(w_event_id_ticket_id_by_total)).alias("total_days_since_event_at_change"),
        F.round(F.col("total_days_since_event_at") - F.avg("total_days_since_event_at").over(w_event_id_event_event_type), 2).alias("total_days_since_event_at_vs_event_id_event_even"),
    )
)

enriched = (
    with_windows.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.id") == F.col("s.event_id"), "inner")
    .join(df_tickets.alias("ti"), F.col("ti.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.ticket_id",
        "s.event_type",
        "s.event_at_ym",
        "s.total_days_since_event_at",
        "s.event_id",
        "s.event_event_type",
        "s.distinct_ticket_id_in_event_id_event_event_type",
        "s.total_days_since_event_at_max_3_prior",
        "s.distinct_event_type_in_event_id_event_event_type",
        "s.event_at_seq",
        "s.total_days_since_event_at_vs_event_id_event_even",
        F.col("t.ticket_id").alias("event_ticket_id"),
        F.col("t.actor").alias("event_actor"),
        F.col("ti.tags").alias("ticket_tags"),
        F.col("ti.channel").alias("ticket_channel"),
    )
)

with_activity = (
    enriched.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type").isNotNull()) & (F.to_date(F.col("t.event_at")) < F.lit("2025-02-12").cast("date")), "left_semi")
    .filter(F.col("event_id") <= 25)
)

result = (
    with_activity
    .select(
        "ticket_id",
        "event_type",
        "event_at_ym",
        "total_days_since_event_at",
        "event_event_type",
        "distinct_ticket_id_in_event_id_event_event_type",
        "distinct_event_type_in_event_id_event_event_type",
        "total_days_since_event_at_vs_event_id_event_even",
        "event_ticket_id",
        "event_actor",
        "ticket_tags",
        "ticket_channel",
        F.substring(F.trim(F.col("event_actor")), 1, 3).alias("event_actor_prefix"),
    )
    .orderBy(
        F.col("total_days_since_event_at").desc(),
        F.col("ticket_channel").asc_nulls_last(),
    )
)
