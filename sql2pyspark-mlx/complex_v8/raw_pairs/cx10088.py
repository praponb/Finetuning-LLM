from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_at_month_by_event_coun = Window.partitionBy("event_at_month").orderBy(F.col("event_count").desc(), "id")
w_event_at_month_by_event_coun_2 = Window.partitionBy("event_at_month").orderBy(F.col("event_count").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id = Window.partitionBy("ticket_id")
w_event_at_month_by_event_coun_3 = Window.partitionBy("event_at_month").orderBy(F.col("event_count").desc())

recent_events = (
    df_ticket_events
    .filter(F.col("event_type").isin("reopened", "resolved", "assigned"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
        F.when(F.col("event_type").isin("resolved", "reopened"), "group_a").when(F.col("event_type") == "escalated", "group_b").otherwise("other").alias("event_type_group"),
        F.month(F.col("event_at")).alias("event_at_month"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.count("*").alias("event_count"))
)

summarized_rows = (
    recent_events.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

windowed = (
    summarized_rows
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "event_type_or_unknown",
        "event_at_month",
        "event_count",
        F.lag("event_at").over(w_event_at_month_by_event_coun).alias("prev_event_at"),
        F.nth_value("event_at_month", 2).over(w_event_at_month_by_event_coun_2).alias("second_event_at_month"),
        F.max("event_count").over(w_ticket_id).alias("ticket_id_max_event_count"),
        F.round(F.cume_dist().over(w_event_at_month_by_event_coun_3), 3).alias("event_count_cume_dist"),
    )
)

combined_details = (
    windowed.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.event_at_month",
        "s.prev_event_at",
        "s.ticket_id_max_event_count",
        "s.event_count_cume_dist",
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.agent_id").alias("ticket_agent_id"),
    )
)

result = (
    combined_details
    .select("id", "ticket_id_max_event_count", "ticket_csat")
    .unpivot(
        ids=["id"],
        values=["ticket_id_max_event_count", "ticket_csat"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
