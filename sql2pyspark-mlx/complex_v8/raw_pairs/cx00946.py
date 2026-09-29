from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("event_type", "ticket_id").orderBy("days_since_event_at_2", "id")

recent = (
    df_ticket_events
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.concat_ws("-", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at")).alias("days_since_event_at_2"),
    )
)

latest = (
    recent
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

joined = (
    latest.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.days_since_event_at",
        "s.event_type_actor_label",
        "s.days_since_event_at_2",
        F.col("t.tags").alias("ticket_tags"),
    )
)

actor_stats = (
    joined
    .groupBy("actor")
    .agg(F.percentile_approx("days_since_event_at", 0.5).alias("median_days_since_event_at"))
)

outliers = (
    joined.alias("s")
    .join(actor_stats.alias("g"), F.col("g.actor") == F.col("s.actor"), "inner")
    .filter(F.col("s.days_since_event_at") >= F.col("g.median_days_since_event_at"))
    .select(
        "s.*",
        "g.median_days_since_event_at",
        F.round(F.col("s.days_since_event_at") - F.col("g.median_days_since_event_at"), 2).alias("days_since_event_at_minus_median"),
    )
)

enriched = (
    outliers
    .select(
        "*",
        F.concat(F.col("event_type"), F.lit("#"), F.col("ticket_id").cast("string")).alias("event_type_ref"),
        F.round(F.col("days_since_event_at_2") / F.nullif(F.col("days_since_event_at"), F.lit(0)), 3).alias("days_since_event_at_2_per_days_since_event_at"),
    )
)

exploded = (
    enriched
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "days_since_event_at",
        "event_type_actor_label",
        "days_since_event_at_2",
        "median_days_since_event_at",
        "days_since_event_at_minus_median",
        "event_type_ref",
        "days_since_event_at_2_per_days_since_event_at",
        F.explode_outer("ticket_tags").alias("ticket_tag"),
    )
)

labeled = (
    exploded
    .filter(F.col("ticket_tag").isin("refund", "upgrade"))
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "event_type_actor_label",
        "days_since_event_at_2",
        "median_days_since_event_at",
        "ticket_tag",
        (F.floor(F.col("days_since_event_at") / 100) * 100).alias("days_since_event_at_bucket"),
        F.coalesce(F.col("days_since_event_at_2"), F.lit(0)).alias("days_since_event_at_2_filled"),
    )
)

result = (
    labeled
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "event_type_actor_label",
        "days_since_event_at_2",
        "median_days_since_event_at",
        "ticket_tag",
        "days_since_event_at_bucket",
        "days_since_event_at_2_filled",
        F.coalesce(F.col("ticket_tag"), F.lit("UNKNOWN")).alias("ticket_tag_or_unknown"),
    )
    .orderBy(
        "event_type",
        F.col("actor").desc(),
        "id",
        "ticket_tag",
    )
    .limit(5)
)
