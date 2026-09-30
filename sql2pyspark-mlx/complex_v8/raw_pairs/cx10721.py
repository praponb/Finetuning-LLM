from pyspark.sql import functions as F

scoped_events = (
    df_ticket_events
    .filter(F.col("ticket_id") >= 11)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.dayofweek(F.col("event_at")).alias("event_at_dayofweek"),
        F.date_format(F.col("event_at"), "yyyy-MM-dd").alias("event_at_label"),
        F.initcap(F.trim(F.col("event_type"))).alias("event_type_clean"),
    )
)

line_level = (
    scoped_events.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type").isNotNull()), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_at_dayofweek",
        "s.event_at_label",
        "s.event_type_clean",
        F.col("t.id").alias("event_id"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.event_at").alias("event_event_at"),
    )
)

result = (
    line_level
    .select(
        "id",
        "event_at",
        "event_at_dayofweek",
        "event_at_label",
        "event_type_clean",
        "event_id",
        "event_event_type",
        "event_event_at",
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
    )
    .orderBy(F.col("event_event_at").desc_nulls_first())
)
