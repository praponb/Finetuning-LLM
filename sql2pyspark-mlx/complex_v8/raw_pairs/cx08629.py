from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_year_running = Window.orderBy("event_at_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_event_at_full = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_year = Window.orderBy("event_at_year", "id")
w_dedup = Window.partitionBy("event_type").orderBy(F.col("event_at_year_quartile").desc(), F.col("id").desc())

prepared = (
    df_ticket_events
    .filter(F.col("actor") == "agent")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_trunc("year", F.col("event_at")).alias("event_at_year"),
        F.to_date(F.col("event_at")).alias("event_at_date"),
        F.when(F.col("event_type").isin("created", "replied"), "group_a").when(F.col("event_type") == "resolved", "group_b").otherwise("other").alias("event_type_group"),
    )
)

with_windows = (
    prepared
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "event_at_year",
        "event_at_date",
        "event_type_group",
        F.first("event_at", ignorenulls=True).over(w_all_by_event_at_year_running).alias("first_known_event_at"),
        F.last("event_at_date").over(w_all_by_event_at_full).alias("last_event_at_date"),
        F.ntile(4).over(w_all_by_event_at_year).alias("event_at_year_quartile"),
    )
)

ticket_id_stats = (
    with_windows
    .groupBy("ticket_id")
    .agg(F.max("event_at_year_quartile").alias("max_event_at_year_quartile"))
)

above_average = (
    with_windows.alias("s")
    .join(ticket_id_stats.alias("g"), F.col("g.ticket_id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("s.event_at_year_quartile") == F.col("g.max_event_at_year_quartile"))
    .select(
        "s.*",
        "g.max_event_at_year_quartile",
        F.round(F.col("s.event_at_year_quartile") - F.col("g.max_event_at_year_quartile"), 2).alias("event_at_year_quartile_minus_max"),
    )
)

unique_rows = (
    above_average
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

with_parent = (
    unique_rows.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_at_year",
        "s.event_at_date",
        "s.event_type_group",
        "s.first_known_event_at",
        "s.event_at_year_quartile",
        "s.max_event_at_year_quartile",
        "s.event_at_year_quartile_minus_max",
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

result = (
    with_parent
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_at_year",
        "event_type_group",
        "first_known_event_at",
        "max_event_at_year_quartile",
        "event_at_year_quartile_minus_max",
        "ticket_csat",
        "ticket_closed_at",
    )
    .orderBy(F.col("event_type_group").asc_nulls_last(), "id")
    .limit(20)
)
