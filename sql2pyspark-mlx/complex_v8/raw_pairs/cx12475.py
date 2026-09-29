from pyspark.sql import functions as F

prepared_events = (
    df_ticket_events
    .filter((~F.col("actor").isin("customer", "system")) | (F.col("event_at") < F.lit("2025-02-24 08:00:00").cast("timestamp")))
)

screened = (
    prepared_events.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left_semi")
)

scored = (
    screened
    .select(
        "*",
        F.concat(F.col("event_type"), F.lit("#"), F.col("ticket_id").cast("string")).alias("event_type_ref"),
        F.upper(F.trim(F.col("event_type"))).alias("event_type_clean"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.min("event_at").alias("first_event_at"))
)

with_counts = (
    scored.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.first_event_at")
)

result = (
    with_counts
    .groupingSets(
        [["actor", "event_type"], ["actor"], []],
        "actor", "event_type",
    )
    .agg(
        F.round(100 * F.count(F.when(F.col("event_type") == "escalated", 1)) / F.count("*"), 1).alias("pct_event_type_escalated"),
        F.count(F.when(F.col("actor") == "agent", 1)).alias("n_actor_agent"),
        F.count(F.when(~F.col("event_type").isin("replied", "resolved"), 1)).alias("n_event_type_not_in_replied_re"),
    )
    .orderBy(F.col("actor").asc_nulls_last(), F.col("event_type").asc_nulls_last())
)
