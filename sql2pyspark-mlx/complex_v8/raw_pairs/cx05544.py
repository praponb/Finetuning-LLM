from pyspark.sql import functions as F

base = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
    )
)

enriched = (
    base
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "*",
        F.concat_ws(": ", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
        F.to_date(F.col("event_at")).alias("event_at_date"),
    )
)

grouped = (
    enriched
    .filter(F.year(F.col("event_at_date")) == 2024)
    .groupBy("actor", "event_type")
    .agg(
        F.round(F.avg("event_type_len"), 2).alias("avg_event_type_len"),
        F.datediff(F.max("event_at_date"), F.min("event_at_date")).alias("event_at_date_span_days"),
        F.avg("event_type_len").alias("avg_event_type_len_check"),
    )
    .filter(F.col("avg_event_type_len_check") > 6)
    .drop("avg_event_type_len_check")
)

tagged = (
    grouped
    .select(
        "actor",
        "event_type",
        "avg_event_type_len",
        "event_at_date_span_days",
        F.coalesce(F.col("event_at_date_span_days"), F.lit(0)).alias("event_at_date_span_days_filled"),
        F.concat_ws(": ", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
    )
)

tagged_2 = (
    tagged
    .select(
        "actor",
        "event_type",
        "event_at_date_span_days",
        "event_at_date_span_days_filled",
        "actor_event_type_label",
        (F.floor(F.col("avg_event_type_len") / 2) * 2).alias("avg_event_type_len_bucket"),
        (F.floor(F.col("avg_event_type_len") / 2) * 2).alias("avg_event_type_len_bucket_2"),
    )
)

result = (
    tagged_2
    .select("actor", "event_type", "actor_event_type_label")
    .orderBy("actor_event_type_label", "event_type")
)
