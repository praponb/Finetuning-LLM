from pyspark.sql import functions as F

valid_events = (
    df_ticket_events
    .filter(F.col("event_type").isin("created", "resolved"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
        F.concat(F.col("actor"), F.lit("#"), F.col("id").cast("string")).alias("actor_ref"),
    )
)

screened = (
    valid_events.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type") == "replied") & (F.col("t.actor") == "agent"), "left_anti")
)

without_matches = (
    screened
    .crossJoin(screened.agg(F.max("event_type_len").alias("overall_max_event_type_len")))
    .filter(F.col("event_type_len") < F.col("overall_max_event_type_len"))
    .drop("overall_max_event_type_len")
)

result = (
    without_matches
    .filter(F.to_date(F.col("event_at")) < F.lit("2025-02-12").cast("date"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "event_type_len",
        "actor_ref",
    )
    .orderBy(F.col("event_at").asc())
)
