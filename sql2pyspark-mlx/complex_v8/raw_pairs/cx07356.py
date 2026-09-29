from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_by_event_at_running = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_priority_by_ticket_cl = Window.partitionBy("ticket_priority").orderBy(F.col("ticket_closed_at").desc_nulls_first())
w_actor_event_type_by_event_at = Window.partitionBy("actor", "event_type").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_ticket_closed_at = Window.orderBy(F.col("ticket_closed_at").asc_nulls_last(), "id")
w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc(), "id")
w_all_by_ticket_closed_at_runn = Window.orderBy(F.col("ticket_closed_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_priority_by_event_at = Window.partitionBy("ticket_priority").orderBy("event_at", "id")
w_ticket_priority_by_event_at_2 = Window.partitionBy("ticket_priority").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_by_event_at_ties = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc())
w_event_at_year_by_last_known = Window.partitionBy(F.year("event_at")).orderBy("last_known_event_at", "id")
w_event_at_year_by_last_known_2 = Window.partitionBy(F.year("event_at")).orderBy("last_known_event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_at_year_by_first_known = Window.partitionBy(F.year("event_at")).orderBy("first_known_ticket_closed_at", "id")
w_next3_ticket_closed_at_month = Window.partitionBy(F.date_trunc("month", "next3_ticket_closed_at"), F.month("next2_ticket_closed_at"))
w_all_by_last_known_event_at_r = Window.orderBy("last_known_event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_last_known_event_at_p = Window.orderBy("last_known_event_at", "id").rowsBetween(-6, -1)

filtered_events = (
    df_ticket_events
    .filter(F.col("actor").isin("agent", "system"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_parent = (
    filtered_events.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.to_date(F.col("t.opened_at")) < F.lit("2025-02-24").cast("date")), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.priority").alias("ticket_priority"),
        "su.agent_name",
        F.col("su.tier").alias("agent_tier"),
    )
)

sequenced = (
    with_parent
    .select(
        "id",
        "event_at",
        "event_type",
        "ticket_closed_at",
        "ticket_priority",
        "agent_tier",
        F.first("event_at", ignorenulls=True).over(w_actor_by_event_at_running).alias("first_known_event_at"),
        F.rank().over(w_ticket_priority_by_ticket_cl).alias("ticket_closed_at_rank"),
        F.first("ticket_closed_at", ignorenulls=True).over(w_actor_event_type_by_event_at).alias("first_known_ticket_closed_at"),
        F.lead("ticket_closed_at", 3).over(w_all_by_ticket_closed_at).alias("next3_ticket_closed_at"),
        F.row_number().over(w_ticket_id_by_event_at).alias("event_at_seq"),
        F.last("event_at", ignorenulls=True).over(w_all_by_ticket_closed_at_runn).alias("last_known_event_at"),
        F.lead("ticket_closed_at", 2).over(w_ticket_priority_by_event_at).alias("next2_ticket_closed_at"),
        F.nth_value("ticket_closed_at", 2).over(w_ticket_priority_by_event_at_2).alias("second_ticket_closed_at"),
        F.dense_rank().over(w_ticket_id_by_event_at_ties).alias("event_at_dense_rank"),
    )
)

labeled = (
    sequenced
    .filter(F.col("event_type").isin("reopened", "replied", "assigned"))
    .select(
        "id",
        "event_at",
        "event_type",
        "ticket_closed_at",
        "agent_tier",
        "first_known_event_at",
        "first_known_ticket_closed_at",
        "next3_ticket_closed_at",
        "event_at_seq",
        "last_known_event_at",
        "next2_ticket_closed_at",
        "second_ticket_closed_at",
        F.when(F.col("event_at_dense_rank") >= 9, "large").when(F.col("event_at_dense_rank") >= 4, "medium").otherwise("small").alias("event_at_dense_rank_tier"),
    )
)

sequenced_2 = (
    labeled
    .select(
        "*",
        F.lead("second_ticket_closed_at").over(w_event_at_year_by_last_known).alias("next_second_ticket_closed_at"),
        F.count(F.lit(1)).over(w_event_at_year_by_last_known_2).alias("running_count"),
        F.row_number().over(w_event_at_year_by_first_known).alias("first_known_ticket_closed_at_seq"),
        F.size(F.collect_set("event_at_dense_rank_tier").over(w_next3_ticket_closed_at_month)).alias("distinct_event_at_dense_rank_tier_in_next3_ticke"),
        F.round(F.col("event_at_seq") / F.nullif(F.sum("event_at_seq").over(w_next3_ticket_closed_at_month), F.lit(0)), 4).alias("event_at_seq_share_of_next3_ticket_closed_at_mon"),
        F.first("event_at_seq", ignorenulls=True).over(w_all_by_last_known_event_at_r).alias("first_known_event_at_seq"),
        F.rank().over(w_event_at_year_by_last_known).alias("last_known_event_at_rank"),
        F.sum("event_at_seq").over(w_all_by_last_known_event_at_p).alias("event_at_seq_sum_6_prior"),
    )
)

result = (
    sequenced_2
    .select(
        "id",
        "event_at",
        "event_type",
        "ticket_closed_at",
        "agent_tier",
        "first_known_ticket_closed_at",
        "next3_ticket_closed_at",
        "event_at_seq",
        "last_known_event_at",
        "next_second_ticket_closed_at",
        "first_known_ticket_closed_at_seq",
        "distinct_event_at_dense_rank_tier_in_next3_ticke",
        "event_at_seq_share_of_next3_ticket_closed_at_mon",
        "first_known_event_at_seq",
        "last_known_event_at_rank",
        "event_at_seq_sum_6_prior",
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
        F.concat(F.col("event_type"), F.lit("#"), F.col("id").cast("string")).alias("event_type_ref"),
    )
    .orderBy(F.col("event_type_ref").asc_nulls_last())
)
