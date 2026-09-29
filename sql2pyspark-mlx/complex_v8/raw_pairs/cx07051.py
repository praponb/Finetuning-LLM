from pyspark.sql import functions as F

cleaned = (
    df_ticket_events
    .filter((F.month(F.col("event_at")).isin(1, 3)) | (F.col("event_type") != "reopened"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

enriched = (
    cleaned
    .select(
        "*",
        F.concat_ws("-", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.concat_ws("-", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count(F.when(~F.col("actor").isin("system", "customer"), 1)).alias("event_actor_count"),
        F.count("*").alias("event_count"),
        F.max("event_at").alias("last_event_at"),
    )
)

summarized_rows = (
    enriched.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_actor_count"), F.lit(0)).alias("event_actor_count"),
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        "t.last_event_at",
    )
)

ticket_id_stats = (
    summarized_rows
    .groupBy("ticket_id")
    .agg(F.percentile_approx("event_count", 0.5).alias("median_event_count"))
)

above_average = (
    summarized_rows.alias("s")
    .join(ticket_id_stats.alias("g"), F.col("g.ticket_id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("s.event_count") >= F.col("g.median_event_count"))
    .select(
        "s.*",
        "g.median_event_count",
        F.round(F.col("s.event_count") - F.col("g.median_event_count"), 2).alias("event_count_minus_median"),
    )
)

exploded_detail = (
    above_average.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.actor",
        "s.event_type_actor_label",
        "s.event_actor_count",
        "s.median_event_count",
        F.col("t.id").alias("event_id"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.actor").alias("event_actor"),
    )
)

qualified = (
    exploded_detail.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type") != "resolved") & (F.month(F.col("t.event_at")).isin(1, 2)), "left_anti")
)

result = (
    qualified
    .groupBy("event_actor", "event_actor_count")
    .pivot("actor", ["customer", "agent"])
    .agg(F.sum("median_event_count"))
    .orderBy("event_actor", "event_actor_count")
)
