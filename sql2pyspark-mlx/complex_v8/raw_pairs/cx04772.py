from pyspark.sql import functions as F

valid = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
        F.date_format(F.col("event_at"), "yyyy-MM").alias("event_at_label"),
        F.initcap(F.trim(F.col("event_type"))).alias("event_type_clean"),
    )
)

without_matches = (
    valid.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.actor") != "customer") & (F.col("t.event_at") >= F.lit("2025-01-31 10:00:00").cast("timestamp")), "left_semi")
)

actor_stats = (
    without_matches
    .groupBy("actor")
    .agg(F.avg("actor_len").alias("avg_actor_len"))
)

compared = (
    without_matches.alias("s")
    .join(actor_stats.alias("g"), F.col("g.actor") == F.col("s.actor"), "inner")
    .filter(F.col("s.actor_len") > F.col("g.avg_actor_len"))
    .select(
        "s.*",
        "g.avg_actor_len",
        F.round(F.col("s.actor_len") - F.col("g.avg_actor_len"), 2).alias("actor_len_minus_avg"),
    )
)

enriched = (
    compared
    .select(
        "id",
        "event_at",
        "actor",
        "actor_len",
        "actor_len_minus_avg",
        F.round(F.col("actor_len_minus_avg") / F.nullif(F.col("actor_len"), F.lit(0)), 3).alias("actor_len_minus_avg_per_actor_len"),
    )
)

result = (
    enriched
    .select(
        "id",
        "event_at",
        "actor",
        "actor_len",
        "actor_len_minus_avg_per_actor_len",
        F.round(F.col("actor_len") / F.nullif(F.col("actor_len_minus_avg"), F.lit(0)), 3).alias("actor_len_per_actor_len_minus_avg"),
        F.date_format(F.col("event_at"), "yyyy-MM").alias("event_at_label"),
    )
    .orderBy("id", F.col("event_at_label").desc())
    .limit(20)
)
