from pyspark.sql import functions as F

valid_events = (
    df_ticket_events
    .filter(F.col("event_type").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.concat_ws("|", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
        F.substring(F.trim(F.col("actor")), 1, 3).alias("actor_prefix"),
    )
)

tagged = (
    valid_events
    .filter(F.col("actor") == "agent")
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        "actor_prefix",
        F.length(F.trim(F.col("event_type_actor_label"))).alias("event_type_actor_label_len"),
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
        F.quarter(F.col("event_at")).alias("event_at_quarter"),
    )
)

with_activity = (
    tagged
    .crossJoin(tagged.agg(F.avg("event_type_actor_label_len").alias("overall_avg_event_type_actor_label_len")))
    .filter(F.col("event_type_actor_label_len") > F.col("overall_avg_event_type_actor_label_len"))
    .drop("overall_avg_event_type_actor_label_len")
)

annotated = with_activity.withColumn("actor_len", F.length(F.trim(F.col("actor"))))

event_type_stats = (
    annotated
    .groupBy("event_type")
    .agg(F.avg("event_at_quarter").alias("avg_event_at_quarter"))
)

compared = (
    annotated.alias("s")
    .join(event_type_stats.alias("g"), F.col("g.event_type") == F.col("s.event_type"), "inner")
    .filter(F.col("s.event_at_quarter") > F.col("g.avg_event_at_quarter"))
    .select(
        "s.*",
        "g.avg_event_at_quarter",
        F.round(F.col("s.event_at_quarter") - F.col("g.avg_event_at_quarter"), 2).alias("event_at_quarter_minus_avg"),
    )
)

totals = (
    compared
    .filter(F.to_date(F.col("event_at")) < F.lit("2025-03-08").cast("date"))
    .groupBy(F.date_trunc("month", "event_at").alias("event_at_month"))
    .agg(
        F.countDistinct("event_type_or_unknown").alias("distinct_event_type_or_unknowns"),
        F.min("avg_event_at_quarter").alias("min_avg_event_at_quarter"),
        F.round(F.stddev("event_type_actor_label_len"), 2).alias("stddev_event_type_actor_label_len"),
        F.round(F.sum("avg_event_at_quarter") / F.nullif(F.sum("event_at_quarter_minus_avg"), F.lit(0)), 3).alias("avg_event_at_quarter_to_event_at_quarter_minus_a"),
        F.countDistinct("actor").alias("distinct_actors"),
        F.sum("event_type_actor_label_len").alias("total_event_type_actor_label_len"),
        F.avg("event_at_quarter_minus_avg").alias("avg_event_at_quarter_minus_avg_check"),
    )
    .filter(F.col("avg_event_at_quarter_minus_avg_check") > -0.8)
    .drop("avg_event_at_quarter_minus_avg_check")
)

filtered = (
    totals
    .crossJoin(totals.agg(F.max("total_event_type_actor_label_len").alias("overall_max_total_event_type_actor_label_len")))
    .filter(F.col("total_event_type_actor_label_len") < F.col("overall_max_total_event_type_actor_label_len"))
    .drop("overall_max_total_event_type_actor_label_len")
)

result = (
    filtered
    .filter(F.col("distinct_actors") <= 9)
    .select(
        "event_at_month",
        "distinct_event_type_or_unknowns",
        "avg_event_at_quarter_to_event_at_quarter_minus_a",
        "distinct_actors",
        "total_event_type_actor_label_len",
        F.coalesce(F.col("distinct_actors"), F.lit(0)).alias("distinct_actors_filled"),
        (F.col("event_at_month") + F.expr("INTERVAL 7 DAYS")).alias("event_at_month_plus_7d"),
    )
    .orderBy(F.col("distinct_actors_filled").desc_nulls_first())
)
