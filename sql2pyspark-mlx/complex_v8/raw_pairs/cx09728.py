from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_opened_at_to_date_by = Window.partitionBy(F.to_date("ticket_opened_at")).orderBy(F.col("event_at").desc(), "id")
w_all_by_event_at_full = Window.orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_opened_at_month_ticke = Window.partitionBy(F.date_trunc("month", "ticket_opened_at"), "ticket_id").orderBy("ticket_closed_at", "id")
w_all_by_event_at_ties = Window.orderBy(F.col("event_at").asc_nulls_last())
w_actor_by_ticket_closed_at = Window.partitionBy("actor").orderBy("ticket_closed_at", "id")
w_ticket_opened_at_month_ticke_2 = Window.partitionBy(F.date_trunc("month", "ticket_opened_at"), "ticket_id").orderBy("ticket_closed_at")
w_ticket_opened_at_month_ticke_3 = Window.partitionBy(F.date_trunc("month", "ticket_opened_at"), "ticket_id").orderBy("ticket_closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at = Window.orderBy(F.col("event_at").asc_nulls_last(), "id")
w_ticket_closed_at_month_actor = Window.partitionBy(F.date_trunc("month", "ticket_closed_at"), "actor").orderBy(F.col("ticket_opened_at").desc(), "id")
w_event_at_rank_second_ticket = Window.partitionBy("event_at_rank", F.year("second_ticket_opened_at")).orderBy(F.col("second_ticket_closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_at_seq_event_at_rank_2 = Window.partitionBy("event_at_seq", "event_at_rank_2")
w_event_at_rank_second_ticket_2 = Window.partitionBy("event_at_rank", F.year("second_ticket_opened_at")).orderBy(F.col("second_ticket_closed_at").desc(), "id")
w_event_at_rank_ticket_closed = Window.partitionBy("event_at_rank", "ticket_closed_at_rank")
w_agent_tier_by_event_at_rank = Window.partitionBy("agent_tier").orderBy(F.col("event_at_rank_2").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_seq_ticket_id_by_fi = Window.partitionBy("event_at_seq", "ticket_id").orderBy("first_event_at", "id")

filtered_events = (
    df_ticket_events
    .filter((F.col("actor").isin("customer", "system")) & (F.to_date(F.col("event_at")) < F.lit("2025-02-06").cast("date")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.substring(F.trim(F.col("actor")), 1, 2).alias("actor_prefix"),
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
    )
)

enriched_ticket = (
    filtered_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.actor_prefix",
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.opened_at").alias("ticket_opened_at"),
        "su.agent_name",
        F.col("su.tier").alias("agent_tier"),
    )
)

with_windows = (
    enriched_ticket
    .select(
        "*",
        F.rank().over(w_ticket_opened_at_to_date_by).alias("event_at_rank"),
        F.nth_value("ticket_opened_at", 2).over(w_all_by_event_at_full).alias("second_ticket_opened_at"),
        F.dense_rank().over(w_ticket_opened_at_month_ticke).alias("ticket_closed_at_dense_rank"),
        F.dense_rank().over(w_all_by_event_at_ties).alias("event_at_dense_rank"),
        F.ntile(4).over(w_actor_by_ticket_closed_at).alias("ticket_closed_at_quartile"),
        F.rank().over(w_ticket_opened_at_month_ticke_2).alias("ticket_closed_at_rank"),
        F.nth_value("ticket_closed_at", 2).over(w_ticket_opened_at_month_ticke_3).alias("second_ticket_closed_at"),
        F.first("event_at").over(w_all_by_event_at_full).alias("first_event_at"),
        F.row_number().over(w_all_by_event_at).alias("event_at_seq"),
        F.round(F.cume_dist().over(w_actor_by_ticket_closed_at), 3).alias("ticket_closed_at_cume_dist"),
        F.round((F.unix_timestamp("ticket_opened_at") - F.unix_timestamp(F.lag("ticket_opened_at").over(w_ticket_closed_at_month_actor))) / 60, 1).alias("minutes_since_prev_ticket_opened_at"),
        F.rank().over(w_all_by_event_at).alias("event_at_rank_2"),
    )
)

leaders = with_windows.filter(F.col("event_at_rank_2") <= 5)

with_trends = (
    leaders
    .select(
        "*",
        F.first("second_ticket_opened_at", ignorenulls=True).over(w_event_at_rank_second_ticket).alias("first_known_second_ticket_opened_at"),
        F.count(F.lit(1)).over(w_event_at_seq_event_at_rank_2).alias("rows_in_event_at_seq_event_at_rank_2"),
        (F.col("event_at_seq") - F.lag("event_at_seq").over(w_event_at_rank_second_ticket_2)).alias("event_at_seq_change"),
        F.count(F.lit(1)).over(w_event_at_rank_ticket_closed).alias("rows_in_event_at_rank_ticket_closed_at_rank"),
        F.last("ticket_closed_at_dense_rank").over(w_agent_tier_by_event_at_rank).alias("last_ticket_closed_at_dense_rank"),
        F.ntile(10).over(w_event_at_seq_ticket_id_by_fi).alias("first_event_at_decile"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "actor_prefix",
        "ticket_tags",
        "ticket_opened_at",
        "agent_tier",
        "event_at_rank",
        "second_ticket_opened_at",
        "event_at_dense_rank",
        "ticket_closed_at_rank",
        "second_ticket_closed_at",
        "first_event_at",
        "event_at_seq",
        "ticket_closed_at_cume_dist",
        "minutes_since_prev_ticket_opened_at",
        "event_at_rank_2",
        "first_known_second_ticket_opened_at",
        "rows_in_event_at_seq_event_at_rank_2",
        "event_at_seq_change",
        "rows_in_event_at_rank_ticket_closed_at_rank",
        "last_ticket_closed_at_dense_rank",
        "first_event_at_decile",
    )
    .orderBy(F.col("event_at").asc_nulls_last())
)
