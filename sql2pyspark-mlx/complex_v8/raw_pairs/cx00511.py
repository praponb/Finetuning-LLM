from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_channel_event_type_by = Window.partitionBy("ticket_channel", "event_type").orderBy(F.col("event_event_at").desc_nulls_first(), "id", "event_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_to_date_by_event_at = Window.partitionBy(F.to_date("event_at")).orderBy("event_at", "id", "event_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_id_by_event_at = Window.partitionBy("event_id").orderBy("event_at", "id", "event_id")
w_latest = Window.partitionBy("ticket_id", "event_event_type").orderBy(F.col("event_event_at").desc(), F.col("id").desc(), F.col("event_id").desc())

valid_events = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) & (F.col("event_at") >= F.lit("2025-01-25 09:00:00").cast("timestamp")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
        F.initcap(F.trim(F.col("actor"))).alias("actor_clean"),
    )
)

with_details_ticket = (
    valid_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.*",
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.csat").alias("ticket_csat"),
    )
)

without_matches = (
    with_details_ticket.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left_semi")
)

with_children = (
    without_matches.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type").isin("resolved", "created")), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.event_type_slug",
        "s.actor_clean",
        "s.ticket_channel",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.event_at").alias("event_event_at"),
    )
)

ranked = (
    with_children
    .select(
        "*",
        F.last("event_event_at").over(w_ticket_channel_event_type_by).alias("last_event_event_at"),
        F.first("event_at").over(w_event_at_to_date_by_event_at).alias("first_event_at"),
        F.lead("event_at", 3).over(w_event_id_by_event_at).alias("next3_event_at"),
    )
)

latest = (
    ranked
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .filter(F.col("ticket_id") >= 49)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "event_type_slug",
        "ticket_channel",
        "event_id",
        "event_event_type",
        "event_event_at",
        "last_event_event_at",
        "first_event_at",
        "next3_event_at",
        F.upper(F.trim(F.col("event_type_slug"))).alias("event_type_slug_clean"),
        F.date_trunc("hour", F.col("event_event_at")).alias("event_event_at_hour"),
    )
    .orderBy(F.col("event_id").desc(), F.col("id").desc())
)
