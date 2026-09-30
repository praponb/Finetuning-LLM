from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_by_event_at_week_r = Window.partitionBy("ticket_id").orderBy("event_at_week", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_event_at_week_moving = Window.orderBy("event_at_week", "id").rowsBetween(-6, Window.currentRow)
w_ticket_id_by_event_at_week_f = Window.partitionBy("ticket_id").orderBy("event_at_week", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_year = Window.partitionBy(F.year("event_at"))
w_actor_event_type_by_event_at = Window.partitionBy("actor", "event_type").orderBy(F.col("event_at_week").desc(), "id").rowsBetween(-6, -1)
w_ticket_id_by_event_at_week = Window.partitionBy("ticket_id").orderBy("event_at_week", "id")
w_all_by_event_at_week = Window.orderBy("event_at_week", "id")

base = (
    df_ticket_events
    .filter((F.col("actor") != "system") & (F.col("ticket_id").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.date_trunc("week", F.col("event_at")).alias("event_at_week"),
    )
)

joined_ticket = (
    base.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.days_since_event_at",
        "s.event_at_week",
        F.col("t.tags").alias("ticket_tags"),
        "su.agent_name",
    )
)

with_trends = (
    joined_ticket
    .select(
        "*",
        F.last("event_at", ignorenulls=True).over(w_ticket_id_by_event_at_week_r).alias("last_known_event_at"),
        F.sum("days_since_event_at").over(w_all_by_event_at_week_moving).alias("days_since_event_at_sum_7"),
        F.nth_value("days_since_event_at", 2).over(w_ticket_id_by_event_at_week_f).alias("second_days_since_event_at"),
        F.count(F.lit(1)).over(w_event_at_year).alias("rows_in_event_at_year"),
        F.round(F.avg("days_since_event_at").over(w_actor_event_type_by_event_at), 2).alias("days_since_event_at_avg_6_prior"),
        F.round(F.col("days_since_event_at") / F.nullif(F.sum("days_since_event_at").over(w_event_at_year), F.lit(0)), 4).alias("days_since_event_at_share_of_event_at_year"),
        F.lead("event_at_week").over(w_ticket_id_by_event_at_week).alias("next_event_at_week"),
        F.lead("days_since_event_at", 1, 0).over(w_all_by_event_at_week).alias("next_days_since_event_at"),
        F.min("days_since_event_at").over(w_event_at_year).alias("event_at_year_min_days_since_event_at"),
    )
)

annotated = (
    with_trends
    .withColumn("event_type_or_unknown", F.coalesce(F.col("event_type"), F.lit("UNKNOWN")))
)

result = (
    annotated
    .filter(F.col("rows_in_event_at_year").between(2, 13))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "days_since_event_at",
        "event_at_week",
        "ticket_tags",
        "agent_name",
        "days_since_event_at_sum_7",
        "second_days_since_event_at",
        "days_since_event_at_avg_6_prior",
        "next_event_at_week",
        "event_at_year_min_days_since_event_at",
        "event_type_or_unknown",
        F.array_join(F.array_sort(F.col("ticket_tags")), ",").alias("ticket_tags_text"),
    )
    .orderBy(F.col("second_days_since_event_at").asc_nulls_last(), "id")
    .limit(5)
)
