from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at = Window.orderBy("event_at", "id")

scoped_events = (
    df_ticket_events
    .filter((F.col("event_type").isin("reopened", "resolved")) & (F.col("actor").isin("agent", "system")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.upper(F.trim(F.col("event_type"))).alias("event_type_clean"),
        F.date_trunc("month", F.col("event_at")).alias("event_at_month"),
        F.month(F.col("event_at")).alias("event_at_month_2"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.max("event_at").alias("last_event_at"))
)

with_stats = (
    scoped_events.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.last_event_at")
)

windowed = (
    with_stats
    .select(
        "id",
        "ticket_id",
        "actor",
        "event_at_month",
        "last_event_at",
        F.round(F.percent_rank().over(w_all_by_event_at), 3).alias("event_at_percent_rank"),
        F.ntile(3).over(w_all_by_event_at).alias("event_at_tercile"),
    )
)

joined_ticket = (
    windowed.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.*",
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

stats = (
    joined_ticket
    .groupBy(
        "ticket_id",
        F.date_trunc("month", "ticket_closed_at").alias("ticket_closed_at_month"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min("event_at_percent_rank").alias("min_event_at_percent_rank"),
        F.array_sort(F.collect_set("actor")).alias("actor_list"),
        F.min("last_event_at").alias("first_last_event_at"),
    )
)

tagged = (
    stats
    .filter(F.col("cnt") < 8)
    .select(
        "*",
        F.to_date(F.col("first_last_event_at")).alias("first_last_event_at_date"),
        F.date_trunc("quarter", F.col("first_last_event_at")).alias("first_last_event_at_quarter"),
    )
)

result = (
    tagged
    .select(
        "ticket_id",
        "ticket_closed_at_month",
        "min_event_at_percent_rank",
        "actor_list",
        "first_last_event_at",
        "first_last_event_at_quarter",
        F.coalesce(F.col("cnt"), F.lit(0)).alias("cnt_filled"),
        F.array_join(F.array_sort(F.col("actor_list")), ",").alias("actor_list_text"),
    )
    .orderBy(
        F.col("cnt_filled").asc_nulls_last(),
        "ticket_id",
        "ticket_closed_at_month",
    )
    .limit(20)
)
