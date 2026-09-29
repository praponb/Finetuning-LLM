from pyspark.sql import functions as F

scoped_events = (
    df_ticket_events
    .filter((F.to_date(F.col("event_at")) < F.lit("2025-02-24").cast("date")) & (F.col("actor") == "system"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "MMM yyyy").alias("event_at_label"),
        (F.col("event_at") + F.expr("INTERVAL 1 DAYS")).alias("event_at_plus_1d"),
        F.date_format(F.col("event_at"), "yyyy-MM-dd HH:00").alias("event_at_label_2"),
    )
)

derived = (
    scoped_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_at_label",
        "event_at_plus_1d",
        F.round((F.unix_timestamp(F.col("event_at")) - F.unix_timestamp(F.col("event_at_plus_1d"))) / 3600, 1).alias("hours_event_at_plus_1d_to_event_at"),
        F.date_format(F.col("event_at"), "MMM yyyy").alias("event_at_label_3"),
        F.hour(F.col("event_at")).alias("event_at_hour"),
    )
)

combined_details_ticket = (
    derived.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.*",
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.channel").alias("ticket_channel"),
    )
)

result = (
    combined_details_ticket
    .groupBy("ticket_id")
    .pivot("ticket_channel", ["email", "web", "phone"])
    .agg(F.sum("hours_event_at_plus_1d_to_event_at"))
    .orderBy("ticket_id")
)
