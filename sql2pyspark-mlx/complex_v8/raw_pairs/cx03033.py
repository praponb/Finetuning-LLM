from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_at_quarter = Window.partitionBy("event_at_quarter")
w_distinct_event_type_in_event = Window.partitionBy("distinct_event_type_in_event_at_quarter").orderBy(F.col("event_at_quarter_avg_event_at_quarter").desc(), "id").rowsBetween(-1, -1)
w_event_at_quarter_by_rows_in = Window.partitionBy("event_at_quarter").orderBy("rows_in_event_at_quarter", "id")
w_event_at_year_event_type = Window.partitionBy("event_at_year", "event_type")
w_all_by_days_since_event_at_f = Window.orderBy("days_since_event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_event_at_year_by_e = Window.partitionBy("ticket_id", "event_at_year").orderBy(F.col("event_at_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_events = (
    df_ticket_events
    .filter(F.col("event_type") == "reopened")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.year(F.col("event_at")).alias("event_at_year"),
        F.quarter(F.col("event_at")).alias("event_at_quarter"),
    )
)

with_trends = (
    prepared_events
    .select(
        "*",
        F.count(F.lit(1)).over(w_event_at_quarter).alias("rows_in_event_at_quarter"),
        F.size(F.collect_set("event_type").over(w_event_at_quarter)).alias("distinct_event_type_in_event_at_quarter"),
        F.round(F.avg("event_at_quarter").over(w_event_at_quarter), 2).alias("event_at_quarter_avg_event_at_quarter"),
    )
)

sequenced = (
    with_trends
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "days_since_event_at",
        "event_at_year",
        "event_at_quarter_avg_event_at_quarter",
        F.max("days_since_event_at").over(w_distinct_event_type_in_event).alias("days_since_event_at_max_1_prior"),
        F.lead("event_at_quarter").over(w_event_at_quarter_by_rows_in).alias("next_event_at_quarter"),
        F.round(F.col("rows_in_event_at_quarter") / F.nullif(F.sum("rows_in_event_at_quarter").over(w_event_at_quarter), F.lit(0)), 4).alias("rows_in_event_at_quarter_share_of_event_at_quart"),
    )
)

sequenced_2 = (
    sequenced
    .select(
        "*",
        F.round(F.col("days_since_event_at") / F.nullif(F.sum("days_since_event_at").over(w_event_at_year_event_type), F.lit(0)), 4).alias("days_since_event_at_share_of_event_at_year_event"),
        F.first("event_at").over(w_all_by_days_since_event_at_f).alias("first_event_at"),
        F.first("days_since_event_at").over(w_ticket_id_event_at_year_by_e).alias("first_days_since_event_at"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.count("*").alias("event_count"), F.count("*").alias("event_count_2"))
)

profiled = (
    sequenced_2.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        F.coalesce(F.col("t.event_count_2"), F.lit(0)).alias("event_count_2"),
    )
)

actor_stats = (
    profiled
    .groupBy("actor")
    .agg(F.avg("event_count_2").alias("avg_event_count_2"))
)

vs_group = (
    profiled.alias("s")
    .join(actor_stats.alias("g"), F.col("g.actor") == F.col("s.actor"), "inner")
    .filter(F.col("s.event_count_2") > F.col("g.avg_event_count_2"))
    .select(
        "s.*",
        "g.avg_event_count_2",
        F.round(F.col("s.event_count_2") - F.col("g.avg_event_count_2"), 2).alias("event_count_2_minus_avg"),
    )
)

event_stats_2 = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count("*").alias("event_count_3"),
        F.max("event_at").alias("last_event_at"),
        F.count("*").alias("event_count_4"),
    )
)

with_counts = (
    vs_group.alias("s")
    .join(event_stats_2.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count_3"), F.lit(0)).alias("event_count_3"),
        "t.last_event_at",
        F.coalesce(F.col("t.event_count_4"), F.lit(0)).alias("event_count_4"),
    )
)

result = (
    with_counts
    .filter(F.col("event_count") < 7)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "event_at_year",
        "days_since_event_at_share_of_event_at_year_event",
        "first_days_since_event_at",
        "event_count_2",
        "avg_event_count_2",
        "event_count_3",
        "event_count_4",
        F.abs(F.col("event_count_2_minus_avg") - 1.6).alias("event_count_2_minus_avg_distance"),
        F.coalesce(F.col("days_since_event_at"), F.lit(0)).alias("days_since_event_at_filled"),
    )
    .orderBy(F.col("event_at").asc_nulls_last())
)
