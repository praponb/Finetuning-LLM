from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("event_actor").orderBy("event_event_at", "id", "event_id")

recent_events = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) | (F.col("event_type").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "yyyy-MM-dd").alias("event_at_label"),
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
    )
)

detail_rows_events = (
    recent_events.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.event_at_label",
        "s.actor_slug",
        "s.event_type_slug",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.event_type").alias("event_event_type"),
    )
)

latest = (
    detail_rows_events
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_details = (
    latest.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.event_at_label",
        "s.actor_slug",
        "s.event_type_slug",
        "s.event_id",
        "s.event_actor",
        "s.event_event_at",
        "s.event_event_type",
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

per_group = (
    with_details
    .groupBy("event_event_type")
    .agg(
        F.sum(F.when(F.col("event_type").isin("reopened", "escalated"), 1).otherwise(0)).alias("n_event_type_in_reopened_escal"),
        F.round(100 * F.count(F.when(~F.col("actor").isin("system", "agent"), 1)) / F.count("*"), 1).alias("pct_actor_not_in_system_agent"),
        F.array_sort(F.collect_set("event_actor")).alias("event_actor_list"),
        F.count(F.when(F.col("event_event_type") == "assigned", 1)).alias("n_event_event_type_assigned"),
    )
)

result = (
    per_group
    .select(
        "event_event_type",
        "n_event_type_in_reopened_escal",
        "pct_actor_not_in_system_agent",
        "event_actor_list",
        "n_event_event_type_assigned",
        F.coalesce(F.col("event_event_type"), F.lit("UNKNOWN")).alias("event_event_type_or_unknown"),
        F.initcap(F.trim(F.col("event_event_type"))).alias("event_event_type_clean"),
    )
    .orderBy(F.col("n_event_type_in_reopened_escal").desc_nulls_first())
)
