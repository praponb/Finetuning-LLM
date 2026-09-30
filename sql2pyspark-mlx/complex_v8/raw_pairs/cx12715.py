from pyspark.sql import functions as F

cleaned_events = (
    df_ticket_events
    .filter(F.col("actor").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.quarter(F.col("event_at")).alias("event_at_quarter"),
    )
)

actor_stats = (
    cleaned_events
    .groupBy("actor")
    .agg(F.percentile_approx("event_at_quarter", 0.5).alias("median_event_at_quarter"))
)

outliers = (
    cleaned_events.alias("s")
    .join(actor_stats.alias("g"), F.col("g.actor") == F.col("s.actor"), "inner")
    .filter(F.col("s.event_at_quarter") >= F.col("g.median_event_at_quarter"))
    .select(
        "s.*",
        "g.median_event_at_quarter",
        F.round(F.col("s.event_at_quarter") - F.col("g.median_event_at_quarter"), 2).alias("event_at_quarter_minus_median"),
    )
)

joined = (
    outliers.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        "s.days_since_event_at",
        "s.event_at_quarter_minus_median",
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.channel").alias("ticket_channel"),
    )
)

grouped = (
    joined
    .filter(F.col("days_since_event_at") < 595)
    .groupBy(
        "ticket_csat",
        F.date_format("event_at", "yyyy-MM").alias("event_at_ym"),
        "actor",
    )
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.min("ticket_csat").alias("min_ticket_csat"),
        F.count("*").alias("n_rows"),
    )
    .filter(F.col("n_rows") >= 2)
)

scored = (
    grouped
    .select(
        "*",
        F.length(F.trim(F.col("event_at_ym"))).alias("event_at_ym_len"),
        F.abs(F.col("min_ticket_csat") - 3).alias("min_ticket_csat_distance"),
    )
)

tagged = (
    scored
    .filter(F.col("ticket_csat") < 5)
    .select(
        "ticket_csat",
        "event_at_ym",
        "actor",
        "last_event_at",
        "min_ticket_csat",
        "n_rows",
        F.greatest(F.col("n_rows"), F.col("event_at_ym_len")).alias("greatest_n_rows_event_at_ym_len"),
    )
)

scored_2 = (
    tagged
    .select(
        "*",
        F.substring(F.trim(F.col("actor")), 1, 3).alias("actor_prefix"),
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown_2"),
    )
)

result = (
    scored_2
    .select(
        "ticket_csat",
        "event_at_ym",
        "actor",
        "min_ticket_csat",
        "n_rows",
        "greatest_n_rows_event_at_ym_len",
        "actor_prefix",
        "actor_or_unknown",
        F.concat_ws("|", F.col("actor_or_unknown_2"), F.col("actor_or_unknown")).alias("actor_or_unknown_2_actor_or_unknown_label"),
        F.lower(F.regexp_replace(F.trim(F.col("event_at_ym")), " ", "_")).alias("event_at_ym_slug"),
    )
)
