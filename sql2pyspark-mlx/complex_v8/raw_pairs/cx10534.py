from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_actor = Window.partitionBy("event_actor")
w_event_type_by_event_event_at = Window.partitionBy("event_type").orderBy(F.col("event_event_at").desc(), "id", "event_id")

base = (
    df_ticket_events
    .filter((F.to_date(F.col("event_at")) < F.lit("2025-02-06").cast("date")) & (F.col("event_type") == "escalated"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
    )
)

enriched = base.withColumn("event_type_len", F.length(F.trim(F.col("event_type"))))

detail_rows = (
    enriched.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.actor_or_unknown",
        "s.event_type_len",
        F.col("t.id").alias("event_id"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.actor").alias("event_actor"),
    )
)

windowed = (
    detail_rows
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "actor_or_unknown",
        "event_id",
        "event_actor",
        F.round(F.avg("event_type_len").over(w_event_actor), 2).alias("event_actor_avg_event_type_len"),
        F.dense_rank().over(w_event_type_by_event_event_at).alias("event_event_at_dense_rank"),
        F.size(F.collect_set("actor_or_unknown").over(w_event_actor)).alias("distinct_actor_or_unknown_in_event_actor"),
    )
)

combined_details = (
    windowed.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_ticket_events.alias("ti"), F.col("ti.id") == F.col("s.event_id"), "left")
    .filter(~F.array_contains(F.col("t.tags"), "bug"))
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.actor_or_unknown",
        "s.event_id",
        "s.event_actor",
        "s.event_actor_avg_event_type_len",
        "s.distinct_actor_or_unknown_in_event_actor",
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.tags").alias("ticket_tags"),
        F.col("ti.event_type").alias("event_event_type"),
        F.col("ti.actor").alias("event_actor_2"),
    )
)

result = (
    combined_details
    .groupBy("event_at", "ticket_id")
    .pivot("event_event_type", ["escalated", "replied", "reopened", "created"])
    .agg(F.avg("distinct_actor_or_unknown_in_event_actor"))
    .orderBy("event_at", "ticket_id")
)
