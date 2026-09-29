from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("ticket_tag").orderBy(F.col("ticket_csat_filled").desc(), F.col("id").desc(), F.col("ticket_tag_pos").desc(), F.col("event_id").desc())

recent_events = (
    df_ticket_events
    .filter(F.col("event_type") == "reopened")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("event_type").isin("resolved", "created"), "group_a").when(F.col("event_type") == "replied", "group_b").otherwise("other").alias("event_type_group"),
        F.concat(F.col("event_type"), F.lit("#"), F.col("id").cast("string")).alias("event_type_ref"),
    )
)

combined_details = (
    recent_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_type_group",
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.csat").alias("ticket_csat"),
    )
)

exploded = (
    combined_details
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "event_type_group",
        "ticket_opened_at",
        "ticket_csat",
        F.posexplode("ticket_tags").alias("ticket_tag_pos", "ticket_tag"),
    )
)

with_children_events = (
    exploded.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.actor",
        "s.event_type_group",
        "s.ticket_opened_at",
        "s.ticket_csat",
        "s.ticket_tag_pos",
        "s.ticket_tag",
        F.col("t.id").alias("event_id"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.event_at").alias("event_event_at"),
    )
)

labeled = (
    with_children_events
    .select(
        "id",
        "actor",
        "event_type_group",
        "ticket_tag_pos",
        "ticket_tag",
        "event_id",
        "event_event_type",
        "event_event_at",
        F.coalesce(F.col("ticket_csat"), F.lit(0)).alias("ticket_csat_filled"),
    )
)

latest_per_key = (
    labeled
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

screened = (
    latest_per_key.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.id") == F.col("s.event_id")) & (F.hour(F.col("t.event_at")).between(6, 8)), "left_anti")
)

result = (
    screened
    .select(
        "id",
        "actor",
        "event_type_group",
        "ticket_tag_pos",
        "ticket_tag",
        "event_id",
        "event_event_type",
        "event_event_at",
        "ticket_csat_filled",
        F.greatest(F.col("ticket_tag_pos"), F.col("ticket_csat_filled")).alias("greatest_ticket_tag_pos_ticket_csat_filled"),
        F.date_format(F.col("event_event_at"), "yyyy-MM").alias("event_event_at_label"),
    )
    .orderBy("ticket_tag")
)
