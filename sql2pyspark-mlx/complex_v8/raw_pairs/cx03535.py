from pyspark.sql import functions as F

cleaned_events = (
    df_ticket_events
    .filter((F.col("event_type") == "assigned") & (F.month(F.col("event_at")).isin(2, 3)))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.initcap(F.trim(F.col("actor"))).alias("actor_clean"),
    )
)

joined = (
    cleaned_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.opened_at").alias("ticket_opened_at"),
    )
)

totals = (
    joined
    .groupBy("ticket_csat", F.year("event_at").alias("event_at_year"), "ticket_id")
    .agg(
        F.countDistinct("actor").alias("distinct_actors"),
        F.count("*").alias("row_count"),
        F.round(F.avg("ticket_csat"), 2).alias("avg_ticket_csat"),
        F.sum(F.when(F.col("event_type") == "escalated", 1).otherwise(0)).alias("n_event_type_escalated"),
    )
)

tagged = (
    totals
    .filter(F.col("distinct_actors").between(2, 8))
    .select(
        "*",
        F.round(F.col("avg_ticket_csat") * 1.5, 2).alias("avg_ticket_csat_adj"),
        F.abs(F.col("n_event_type_escalated") - 5).alias("n_event_type_escalated_distance"),
    )
)

filtered = (
    tagged.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (~F.col("t.event_type").isin("assigned", "escalated")) & (F.col("t.actor").isNotNull()), "left_semi")
)

result = (
    filtered
    .select(
        "ticket_csat",
        "row_count",
        "avg_ticket_csat",
        "n_event_type_escalated",
        "n_event_type_escalated_distance",
        F.when(F.col("row_count") >= 7, "A").when(F.col("row_count") >= 4, "B").otherwise("C").alias("row_count_tier"),
        F.abs(F.col("ticket_csat") - 4).alias("ticket_csat_distance"),
    )
    .orderBy(
        F.col("row_count").asc_nulls_last(),
        F.col("n_event_type_escalated_distance").desc(),
    )
)
