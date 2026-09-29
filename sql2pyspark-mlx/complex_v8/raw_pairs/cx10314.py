from pyspark.sql import functions as F

valid = (
    df_ticket_events
    .filter(F.hour(F.col("event_at")).between(10, 16))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.year(F.col("event_at")).alias("event_at_year"),
        F.to_date(F.col("event_at")).alias("event_at_date"),
    )
)

derived = (
    valid
    .filter(F.col("event_at_year") < 2026)
    .select(
        "*",
        F.abs(F.col("event_at_year") - 2025).alias("event_at_year_distance"),
        F.abs(F.col("event_at_year") - 2025).alias("event_at_year_distance_2"),
        F.concat_ws("|", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.max("event_at").alias("last_event_at"))
)

summarized_rows = (
    derived.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.last_event_at")
)

grouped = (
    summarized_rows
    .filter(F.col("actor") == "customer")
    .groupBy(F.year("event_at").alias("event_at_year_2"), "event_type")
    .agg(
        F.count("*").alias("row_count"),
        F.sum("event_at_year").alias("total_event_at_year"),
        F.sum(F.when(F.col("event_type").isin("assigned", "escalated", "replied"), F.col("event_at_year"))).alias("event_at_year_event_type_in_assigned_escal"),
        F.round(F.sum("event_at_year_distance_2") / F.nullif(F.sum("event_at_year_distance"), F.lit(0)), 3).alias("event_at_year_distance_2_to_event_at_year_distan"),
        F.round(F.avg("event_at_year"), 2).alias("avg_event_at_year"),
        F.max("last_event_at").alias("last_last_event_at"),
    )
    .filter(F.col("row_count") >= 2)
)

without_matches = (
    grouped
    .crossJoin(grouped.agg(F.min("event_at_year_event_type_in_assigned_escal").alias("overall_min_event_at_year_event_type_in_assigned")))
    .filter(F.col("event_at_year_event_type_in_assigned_escal") > F.col("overall_min_event_at_year_event_type_in_assigned"))
    .drop("overall_min_event_at_year_event_type_in_assigned")
)

result = (
    without_matches
    .select(
        "event_at_year_2",
        "row_count",
        "total_event_at_year",
        "event_at_year_event_type_in_assigned_escal",
        "event_at_year_distance_2_to_event_at_year_distan",
        "last_last_event_at",
        (F.floor(F.col("total_event_at_year") / 5000) * 5000).alias("total_event_at_year_bucket"),
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
    )
    .orderBy(
        F.col("row_count").desc_nulls_first(),
        F.col("event_type_len").asc_nulls_last(),
    )
)
