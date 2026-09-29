from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("actor").orderBy(F.col("event_at").desc(), F.col("id").desc())
w_last_event_at_month_ticket_i = Window.partitionBy(F.month("last_event_at"), "ticket_id")
w_actor_event_type_by_event_at = Window.partitionBy("actor", "event_type").orderBy("event_at", "id")

recent = (
    df_ticket_events
    .filter((F.col("event_type") == "resolved") & (F.hour(F.col("event_at")).between(6, 10)))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

latest_per_key = (
    recent
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.max("event_at").alias("last_event_at_2"),
    )
)

with_counts = (
    latest_per_key.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.last_event_at", "t.last_event_at_2")
)

metrics = (
    with_counts
    .select(
        "*",
        F.round(F.col("days_since_event_at") / F.nullif(F.sum("days_since_event_at").over(w_last_event_at_month_ticket_i), F.lit(0)), 4).alias("days_since_event_at_share_of_last_event_at_month"),
        F.round(F.cume_dist().over(w_actor_event_type_by_event_at), 3).alias("event_at_cume_dist"),
    )
)

with_activity = (
    metrics.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left_semi")
    .filter(F.col("event_type") == "created")
)

line_level_events = (
    with_activity.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.days_since_event_at",
        "s.last_event_at",
        "s.last_event_at_2",
        "s.days_since_event_at_share_of_last_event_at_month",
        F.col("t.id").alias("event_id"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.event_type").alias("event_event_type"),
    )
)

enriched = (
    line_level_events
    .select(
        "*",
        F.date_format(F.col("last_event_at"), "yyyy-MM-dd").alias("last_event_at_label"),
        F.month(F.col("last_event_at")).alias("last_event_at_month"),
    )
)

result = (
    enriched
    .filter(F.col("last_event_at_label").isNotNull())
    .select(
        "id",
        "last_event_at",
        "event_id",
        "event_event_at",
        "event_event_type",
        "last_event_at_label",
        "last_event_at_month",
        F.when(F.col("days_since_event_at") >= 455, "HIGH").when(F.col("days_since_event_at") >= 245, "MEDIUM").otherwise("LOW").alias("days_since_event_at_tier"),
    )
)
