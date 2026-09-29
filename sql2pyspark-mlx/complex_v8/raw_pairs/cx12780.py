from pyspark.sql import functions as F

prepared_events = (
    df_ticket_events
    .filter(F.col("event_type") == "reopened")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws("-", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
        F.concat_ws("|", F.col("actor"), F.col("event_type")).alias("actor_event_type_label_2"),
    )
)

scored = (
    prepared_events
    .filter(F.col("actor_event_type_label").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor_event_type_label_2",
        F.quarter(F.col("event_at")).alias("event_at_quarter"),
    )
)

result = (
    scored
    .groupingSets(
        [["event_type", "event_at_quarter"], ["event_type"], ["event_at_quarter"], []],
        "event_type", "event_at_quarter",
    )
    .agg(
        F.grouping("event_type").alias("is_total_event_type"),
        F.count("*").alias("n_rows"),
        F.round(F.avg("event_at_quarter"), 2).alias("avg_event_at_quarter"),
        F.round(100 * F.count(F.when(F.col("event_type") == "resolved", 1)) / F.count("*"), 1).alias("pct_event_type_resolved"),
    )
    .orderBy(
        F.col("event_type").asc_nulls_last(),
        F.col("event_at_quarter").asc_nulls_last(),
    )
)
