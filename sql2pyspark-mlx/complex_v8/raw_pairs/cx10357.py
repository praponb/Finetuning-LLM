from pyspark.sql import functions as F

cleaned = (
    df_ticket_events
    .filter((~F.col("actor").isin("agent", "customer")) | (F.col("ticket_id").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
        F.concat_ws(": ", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
    )
)

enriched_ticket = (
    cleaned.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select("s.*", F.col("t.requester_id").alias("ticket_requester_id"))
)

annotated = (
    enriched_ticket
    .select(
        "id",
        "event_at",
        "actor",
        "actor_len",
        F.date_format(F.col("event_at"), "yyyy-MM").alias("event_at_label"),
        (F.floor(F.col("ticket_requester_id") / 5) * 5).alias("ticket_requester_id_bucket"),
        F.round(F.col("ticket_requester_id") / F.nullif(F.col("actor_len"), F.lit(0)), 3).alias("ticket_requester_id_per_actor_len"),
    )
)

tagged = (
    annotated
    .filter(F.col("ticket_requester_id_bucket") > 8)
    .select(
        "*",
        F.to_date(F.col("event_at")).alias("event_at_date"),
        F.concat_ws("-", F.col("actor"), F.col("event_at_label")).alias("actor_event_at_label_label"),
        F.substring(F.trim(F.col("actor")), 1, 4).alias("actor_prefix"),
    )
)

result = (
    tagged
    .select(
        "id",
        "event_at",
        "actor",
        "actor_len",
        "event_at_label",
        "ticket_requester_id_bucket",
        "ticket_requester_id_per_actor_len",
        "event_at_date",
        "actor_event_at_label_label",
        "actor_prefix",
        F.initcap(F.trim(F.col("actor_prefix"))).alias("actor_prefix_clean"),
        F.lower(F.regexp_replace(F.trim(F.col("event_at_label")), " ", "_")).alias("event_at_label_slug"),
    )
)
