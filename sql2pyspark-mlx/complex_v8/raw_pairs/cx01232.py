from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_running = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_event_at = Window.orderBy("event_at", "id")
w_last_event_at_year_by_max_da = Window.partitionBy(F.year("last_event_at")).orderBy(F.col("max_days_since_prev_event_at_date").asc_nulls_last(), "id", "event_id")
w_last_event_at_year_by_max_da_2 = Window.partitionBy(F.year("last_event_at")).orderBy(F.col("max_days_since_prev_event_at_date").asc_nulls_last())

base = (
    df_ticket_events
    .filter((F.col("event_type") == "escalated") & (F.col("actor").isin("system", "agent")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.to_date(F.col("event_at")).alias("event_at_date"),
    )
)

with_windows = (
    base
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        F.last("event_at_date", ignorenulls=True).over(w_all_by_event_at_running).alias("last_known_event_at_date"),
        F.datediff(F.col("event_at_date"), F.lag("event_at_date").over(w_all_by_event_at)).alias("days_since_prev_event_at_date"),
        F.row_number().over(w_all_by_event_at).alias("rn"),
    )
)

actor_stats = (
    with_windows
    .groupBy("actor")
    .agg(F.max("days_since_prev_event_at_date").alias("max_days_since_prev_event_at_date"))
)

outliers = (
    with_windows.alias("s")
    .join(actor_stats.alias("g"), F.col("g.actor") == F.col("s.actor"), "inner")
    .filter(F.col("s.days_since_prev_event_at_date") == F.col("g.max_days_since_prev_event_at_date"))
    .select(
        "s.*",
        "g.max_days_since_prev_event_at_date",
        F.round(F.col("s.days_since_prev_event_at_date") - F.col("g.max_days_since_prev_event_at_date"), 2).alias("days_since_prev_event_at_date_minus_max"),
    )
)

combined_details = (
    outliers.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        "s.days_since_prev_event_at_date",
        "s.rn",
        "s.max_days_since_prev_event_at_date",
        "s.days_since_prev_event_at_date_minus_max",
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.tags").alias("ticket_tags"),
        "su.agent_name",
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count("*").alias("event_count"),
        F.max("event_at").alias("last_event_at"),
        F.max("event_at").alias("last_event_at_2"),
    )
)

profiled = (
    combined_details.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        "t.last_event_at",
        "t.last_event_at_2",
    )
)

detail_rows = (
    profiled.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.actor") == "system"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.days_since_prev_event_at_date",
        "s.rn",
        "s.max_days_since_prev_event_at_date",
        "s.ticket_priority",
        "s.ticket_tags",
        "s.agent_name",
        "s.last_event_at",
        "s.last_event_at_2",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        "t.event_type",
        F.col("t.event_at").alias("event_event_at"),
    )
)

with_windows_2 = (
    detail_rows
    .select(
        "id",
        "ticket_id",
        "days_since_prev_event_at_date",
        "rn",
        "max_days_since_prev_event_at_date",
        "ticket_priority",
        "agent_name",
        "last_event_at",
        "last_event_at_2",
        "event_id",
        "event_actor",
        "event_type",
        "event_event_at",
        F.ntile(10).over(w_last_event_at_year_by_max_da).alias("max_days_since_prev_event_at_date_decile"),
        F.round(F.percent_rank().over(w_last_event_at_year_by_max_da_2), 3).alias("max_days_since_prev_event_at_date_percent_rank"),
    )
)

result = (
    with_windows_2
    .select(
        "id",
        "event_id",
        "days_since_prev_event_at_date",
        "max_days_since_prev_event_at_date_decile",
        "rn",
    )
    .unpivot(
        ids=["id", "event_id"],
        values=["days_since_prev_event_at_date", "max_days_since_prev_event_at_date_decile", "rn"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
