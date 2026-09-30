from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_full = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_priority_by_event_at = Window.partitionBy("priority").orderBy("event_at", "id")
w_event_at_year_ticket_id = Window.partitionBy(F.year("event_at"), "ticket_id")
w_event_type_by_event_at = Window.partitionBy("event_type").orderBy(F.col("event_at").desc(), "id")

covered = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 1 DAYS"))),
        "inner",
    )
    .select(
        "e.id",
        "e.ticket_id",
        "e.event_at",
        "e.event_type",
        "t.priority",
        "t.agent_id",
        F.round((F.unix_timestamp("e.event_at") - F.unix_timestamp("t.opened_at")) / 3600, 2).alias("hours_since_open"),
    )
)

sequenced = (
    covered
    .select(
        "*",
        F.nth_value("event_at", 2).over(w_all_by_event_at_full).alias("second_event_at"),
        F.round(F.cume_dist().over(w_priority_by_event_at), 3).alias("event_at_cume_dist"),
    )
)

with_trends = (
    sequenced
    .select(
        "id",
        "event_at",
        "event_type",
        "priority",
        "agent_id",
        "hours_since_open",
        "second_event_at",
        F.round(F.col("event_at_cume_dist") - F.avg("event_at_cume_dist").over(w_event_at_year_ticket_id), 2).alias("event_at_cume_dist_vs_event_at_year_ticket_id_av"),
        F.lead("event_at", 3).over(w_event_type_by_event_at).alias("next3_event_at"),
        F.round(F.col("event_at_cume_dist") / F.nullif(F.sum("event_at_cume_dist").over(w_event_at_year_ticket_id), F.lit(0)), 4).alias("event_at_cume_dist_share_of_event_at_year_ticket"),
        F.lag("second_event_at", 3).over(w_event_type_by_event_at).alias("prev3_second_event_at"),
        F.rank().over(w_event_type_by_event_at).alias("event_at_rank"),
        F.ntile(5).over(w_event_type_by_event_at).alias("event_at_quintile"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(F.round(F.avg("requester_id"), 2).alias("avg_ticket_requester_id"))
)

summarized_rows = (
    with_trends.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select("s.*", "t.avg_ticket_requester_id")
)

result = (
    summarized_rows
    .select(
        "id",
        "event_at",
        "priority",
        "agent_id",
        "hours_since_open",
        "second_event_at",
        "event_at_cume_dist_vs_event_at_year_ticket_id_av",
        "next3_event_at",
        "prev3_second_event_at",
        "event_at_quintile",
        "avg_ticket_requester_id",
        F.when(F.col("event_type").isin("resolved", "assigned"), "group_a").when(F.col("event_type") == "reopened", "group_b").otherwise("other").alias("event_type_group"),
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
    )
    .orderBy("prev3_second_event_at")
)
