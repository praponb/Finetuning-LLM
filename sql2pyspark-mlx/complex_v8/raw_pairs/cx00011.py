from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_at_year_ticket_agent_i = Window.partitionBy(F.year("event_at"), "ticket_agent_id").orderBy("event_at", "id")
w_all_by_event_at = Window.orderBy("event_at", "id")
w_ticket_agent_id_ticket_close = Window.partitionBy("ticket_agent_id", F.to_date("ticket_closed_at")).orderBy(F.col("event_at").desc(), "id")
w_ticket_agent_id_ticket_id_by = Window.partitionBy("ticket_agent_id", "ticket_id").orderBy(F.col("ticket_closed_at").desc(), "id")
w_ticket_id_ticket_agent_id_by = Window.partitionBy("ticket_id", "ticket_agent_id").orderBy(F.col("event_at").desc_nulls_first(), "id")
w_ticket_agent_id_ticket_close_2 = Window.partitionBy("ticket_agent_id", F.to_date("ticket_closed_at")).orderBy(F.col("event_at").desc())
w_event_at_year_ticket_agent_i_2 = Window.partitionBy(F.year("event_at"), "ticket_agent_id").orderBy("event_at")
w_ticket_agent_id_ticket_close_3 = Window.partitionBy("ticket_agent_id", F.to_date("ticket_closed_at")).orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_next3_event_at_month_event_a = Window.partitionBy(F.month("next3_event_at"), "event_at_dense_rank").orderBy(F.col("event_at_percent_rank").desc_nulls_first(), "id")
w_ticket_agent_id = Window.partitionBy("ticket_agent_id")
w_event_at_rank_2_by_next3_eve = Window.partitionBy("event_at_rank_2").orderBy("next3_event_at", "id")

scoped_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.substring(F.trim(F.col("event_type")), 1, 3).alias("event_type_prefix"),
    )
)

with_parent_ticket = (
    scoped_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type_prefix",
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

sequenced = (
    with_parent_ticket
    .select(
        "*",
        F.round(F.cume_dist().over(w_event_at_year_ticket_agent_i), 3).alias("event_at_cume_dist"),
        F.dense_rank().over(w_all_by_event_at).alias("event_at_dense_rank"),
        F.lag("event_at", 3).over(w_ticket_agent_id_ticket_close).alias("prev3_event_at"),
        F.lead("event_at", 3).over(w_ticket_agent_id_ticket_id_by).alias("next3_event_at"),
        F.lead("ticket_closed_at").over(w_ticket_id_ticket_agent_id_by).alias("next_ticket_closed_at"),
        F.row_number().over(w_ticket_agent_id_ticket_close).alias("rn"),
        F.rank().over(w_ticket_id_ticket_agent_id_by).alias("event_at_rank"),
        F.rank().over(w_ticket_agent_id_ticket_close_2).alias("event_at_rank_2"),
        F.round(F.percent_rank().over(w_ticket_agent_id_ticket_close), 3).alias("event_at_percent_rank"),
        F.lag("ticket_closed_at", 2).over(w_ticket_id_ticket_agent_id_by).alias("prev2_ticket_closed_at"),
        F.rank().over(w_event_at_year_ticket_agent_i_2).alias("event_at_rank_3"),
        F.row_number().over(w_event_at_year_ticket_agent_i).alias("rn_2"),
        F.nth_value("ticket_closed_at", 2).over(w_ticket_agent_id_ticket_close_3).alias("second_ticket_closed_at"),
        F.dense_rank().over(w_ticket_id_ticket_agent_id_by).alias("event_at_dense_rank_2"),
        F.lead("event_at", 2).over(w_ticket_agent_id_ticket_id_by).alias("next2_event_at"),
    )
)

sequenced_2 = (
    sequenced
    .select(
        "*",
        F.round((F.unix_timestamp("second_ticket_closed_at") - F.unix_timestamp(F.lag("second_ticket_closed_at").over(w_next3_event_at_month_event_a))) / 60, 1).alias("minutes_since_prev_second_ticket_closed_at"),
        F.max("event_at_rank_2").over(w_ticket_agent_id).alias("ticket_agent_id_max_event_at_rank_2"),
        F.lead("event_at").over(w_event_at_rank_2_by_next3_eve).alias("next_event_at"),
        F.max("event_at_cume_dist").over(w_ticket_agent_id).alias("ticket_agent_id_max_event_at_cume_dist"),
    )
)

result = (
    sequenced_2
    .filter(F.col("ticket_closed_at") < F.lit("2025-02-19 21:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "ticket_closed_at",
        "event_at_cume_dist",
        "event_at_dense_rank",
        "prev3_event_at",
        "next_ticket_closed_at",
        "rn",
        "event_at_rank",
        "event_at_rank_2",
        "event_at_percent_rank",
        "prev2_ticket_closed_at",
        "rn_2",
        "second_ticket_closed_at",
        "next2_event_at",
        "minutes_since_prev_second_ticket_closed_at",
        "next_event_at",
        "ticket_agent_id_max_event_at_cume_dist",
        (F.col("next2_event_at") + F.expr("INTERVAL 1 DAYS")).alias("next2_event_at_plus_1d"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("next2_event_at")).alias("days_since_next2_event_at"),
    )
    .orderBy("second_ticket_closed_at", "id")
    .limit(10)
)
