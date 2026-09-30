from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc(), F.col("id").desc())

filtered = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws(" / ", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
    )
)

screened = (
    filtered.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_at") >= F.lit("2025-01-25 13:00:00").cast("timestamp")), "left_anti")
)

latest_per_key = (
    screened
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

enriched = (
    latest_per_key
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "*",
        F.concat(F.col("event_type"), F.lit("#"), F.col("id").cast("string")).alias("event_type_ref"),
        (F.col("event_at") + F.expr("INTERVAL 1 DAYS")).alias("event_at_plus_1d"),
    )
)

combined_details = (
    enriched.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.event_type_actor_label",
        "s.event_type_ref",
        "s.event_at_plus_1d",
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.requester_id").alias("ticket_requester_id"),
    )
)

result = (
    combined_details
    .select(
        "id",
        "ticket_id",
        "event_type",
        "event_type_actor_label",
        "event_at_plus_1d",
        "ticket_closed_at",
        "ticket_requester_id",
        F.concat_ws(" / ", F.col("event_type_actor_label"), F.col("event_type")).alias("event_type_actor_label_event_type_label"),
    )
    .orderBy(F.col("ticket_closed_at").desc_nulls_first())
)
