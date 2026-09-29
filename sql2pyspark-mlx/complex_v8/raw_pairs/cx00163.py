from pyspark.sql import functions as F

filtered = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) & (F.col("actor").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "yyyy-MM-dd HH:00").alias("event_at_label"),
    )
)

stats = (
    filtered
    .groupBy("event_type")
    .agg(
        F.count("*").alias("cnt"),
        F.min("event_at").alias("first_event_at"),
        F.countDistinct("ticket_id").alias("distinct_ticket_ids"),
    )
)

enriched = (
    stats
    .select(
        "event_type",
        "cnt",
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
        (F.col("cnt") - F.col("distinct_ticket_ids")).alias("cnt_minus_distinct_ticket_ids"),
        F.to_date(F.col("first_event_at")).alias("first_event_at_date"),
    )
)

result = (
    enriched
    .select("event_type", "cnt", "event_type_len")
    .unpivot(
        ids=["event_type"],
        values=["cnt", "event_type_len"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .orderBy("event_type", "measure")
)
