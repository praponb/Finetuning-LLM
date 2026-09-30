from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at = Window.orderBy(F.col("event_at").desc(), "id")
w_all_by_hours_since_open_runn = Window.orderBy("hours_since_open", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type = Window.partitionBy("event_type")
w_all_by_hours_since_open = Window.orderBy("hours_since_open", "id")
w_all_by_event_at_prior_6 = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(-6, -1)

joined_ranges = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 3 DAYS"))),
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

windowed = (
    joined_ranges
    .select(
        "id",
        "priority",
        "hours_since_open",
        F.round(F.cume_dist().over(w_all_by_event_at), 3).alias("event_at_cume_dist"),
        F.min("hours_since_open").over(w_all_by_hours_since_open_runn).alias("min_hours_since_open_so_far"),
        F.size(F.collect_set("priority").over(w_event_type)).alias("distinct_priority_in_event_type"),
        F.lead("hours_since_open").over(w_all_by_event_at).alias("next_hours_since_open"),
        F.lead("hours_since_open", 3, 0).over(w_all_by_hours_since_open).alias("next3_hours_since_open"),
        F.round(F.avg("hours_since_open").over(w_all_by_event_at_prior_6), 2).alias("hours_since_open_avg_6_prior"),
    )
)

result = (
    windowed
    .select(
        "id",
        "priority",
        "hours_since_open",
        "min_hours_since_open_so_far",
        "distinct_priority_in_event_type",
        "next_hours_since_open",
        "next3_hours_since_open",
        "hours_since_open_avg_6_prior",
        F.round(F.col("next_hours_since_open") / F.nullif(F.col("hours_since_open"), F.lit(0)), 3).alias("next_hours_since_open_per_hours_since_open"),
    )
)
