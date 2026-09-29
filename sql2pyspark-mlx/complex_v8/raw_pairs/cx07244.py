from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_event_type = Window.partitionBy("actor", "event_type")
w_event_type_event_at_week_yea = Window.partitionBy("event_type", F.year("event_at_week"))
w_actor_event_at_week_month_by = Window.partitionBy("actor", F.month("event_at_week")).orderBy(F.col("agent_hired_on").asc_nulls_last())
w_event_at_week_month_event_ty = Window.partitionBy(F.date_trunc("month", "event_at_week"), "event_type").orderBy(F.col("agent_hired_on").desc(), "id")
w_ticket_id_by_ticket_requeste = Window.partitionBy("ticket_id").orderBy("ticket_requester_id", "id")
w_actor_event_at_week_month_by_2 = Window.partitionBy("actor", F.month("event_at_week")).orderBy(F.col("agent_hired_on").asc_nulls_last(), "id")
w_ticket_id_by_ticket_requeste_2 = Window.partitionBy("ticket_id").orderBy("ticket_requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_event_at_week_month_by_3 = Window.partitionBy("actor", F.month("event_at_week")).orderBy(F.col("agent_hired_on").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_week_month_by_prev3 = Window.partitionBy(F.month("event_at_week")).orderBy("prev3_event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_agent_hired_on_full = Window.orderBy(F.col("agent_hired_on").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_week_month_by_prev3_2 = Window.partitionBy(F.month("event_at_week")).orderBy("prev3_event_at", "id")
w_event_type_by_actor_event_ty = Window.partitionBy("event_type").orderBy("actor_event_type_avg_ticket_requester_id")
w_event_type_by_actor_event_ty_2 = Window.partitionBy("event_type").orderBy("actor_event_type_avg_ticket_requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_ticket_events
    .filter((F.col("event_at") >= F.lit("2025-01-25 13:00:00").cast("timestamp")) & (F.col("event_type").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_trunc("week", F.col("event_at")).alias("event_at_week"),
    )
)

enriched = (
    recent.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .filter(F.col("t.requester_id").between(5, 18))
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_at_week",
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("su.hired_on").alias("agent_hired_on"),
        "su.agent_name",
    )
)

ranked = (
    enriched
    .select(
        "*",
        F.round(F.col("ticket_requester_id") / F.nullif(F.sum("ticket_requester_id").over(w_actor_event_type), F.lit(0)), 4).alias("ticket_requester_id_share_of_actor_event_type"),
        F.count(F.lit(1)).over(w_event_type_event_at_week_yea).alias("rows_in_event_type_event_at_week_year"),
        F.dense_rank().over(w_actor_event_at_week_month_by).alias("agent_hired_on_dense_rank"),
        F.round((F.unix_timestamp("event_at_week") - F.unix_timestamp(F.lag("event_at_week").over(w_event_at_week_month_event_ty))) / 60, 1).alias("minutes_since_prev_event_at_week"),
        F.round(F.col("ticket_requester_id") - F.avg("ticket_requester_id").over(w_event_type_event_at_week_yea), 2).alias("ticket_requester_id_vs_event_type_event_at_week_"),
        F.round(F.avg("ticket_requester_id").over(w_actor_event_type), 2).alias("actor_event_type_avg_ticket_requester_id"),
        F.lag("event_at", 3).over(w_ticket_id_by_ticket_requeste).alias("prev3_event_at"),
        F.lead("event_at_week").over(w_actor_event_at_week_month_by_2).alias("next_event_at_week"),
        F.rank().over(w_actor_event_at_week_month_by).alias("agent_hired_on_rank"),
        F.last("agent_hired_on").over(w_ticket_id_by_ticket_requeste_2).alias("last_agent_hired_on"),
        F.nth_value("event_at_week", 2).over(w_actor_event_at_week_month_by_3).alias("second_event_at_week"),
    )
)

leaders = (
    ranked
    .filter(F.col("agent_hired_on_dense_rank") == 1)
    .drop("agent_hired_on_dense_rank")
)

metrics = (
    leaders
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "event_at_week",
        "agent_hired_on",
        "agent_name",
        "minutes_since_prev_event_at_week",
        "prev3_event_at",
        "next_event_at_week",
        "last_agent_hired_on",
        "second_event_at_week",
        F.nth_value("agent_hired_on_rank", 2).over(w_event_at_week_month_by_prev3).alias("second_agent_hired_on_rank"),
        F.nth_value("second_event_at_week", 2).over(w_all_by_agent_hired_on_full).alias("second_second_event_at_week"),
        F.lead("second_event_at_week", 2).over(w_event_at_week_month_by_prev3_2).alias("next2_second_event_at_week"),
        F.round(F.percent_rank().over(w_event_type_by_actor_event_ty), 3).alias("actor_event_type_avg_ticket_requester_id_percent"),
        F.count(F.lit(1)).over(w_event_type_by_actor_event_ty_2).alias("running_count"),
    )
)

result = (
    metrics
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "agent_name",
        "prev3_event_at",
        "next_event_at_week",
        "last_agent_hired_on",
        "second_agent_hired_on_rank",
        "second_second_event_at_week",
        "next2_second_event_at_week",
        "actor_event_type_avg_ticket_requester_id_percent",
    )
    .orderBy(F.col("actor").desc())
)
