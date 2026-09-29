from pyspark.sql import functions as F

prepared_events = (
    df_ticket_events
    .filter((F.col("ticket_id") >= 22) & (~F.col("actor").isin("customer", "system")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        (F.col("event_at") + F.expr("INTERVAL 30 MINUTES")).alias("event_at_plus_30m"),
        (F.col("event_at") + F.expr("INTERVAL 1 DAYS")).alias("event_at_plus_1d"),
        F.concat_ws("-", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
    )
)

tagged = (
    prepared_events
    .select(
        "*",
        F.substring(F.trim(F.col("actor")), 1, 2).alias("actor_prefix"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
    )
)

result = (
    tagged
    .groupingSets(
        [["event_type", "actor"], ["event_type"], []],
        "event_type", "actor",
    )
    .agg(F.max("event_at_plus_30m").alias("last_event_at_plus_30m"))
)
