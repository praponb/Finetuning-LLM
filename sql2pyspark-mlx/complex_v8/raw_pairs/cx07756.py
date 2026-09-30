from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_cnt = Window.orderBy("cnt", "event_at_ym", "agent_id")
w_all_by_cnt_prior_2 = Window.orderBy("cnt", "event_at_ym", "agent_id").rowsBetween(-2, -1)
w_all_by_cnt_ties = Window.orderBy("cnt")

covered = (
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

grouped = (
    covered
    .filter(F.col("agent_id").isNotNull())
    .groupBy(F.date_format("event_at", "yyyy-MM").alias("event_at_ym"), "agent_id")
    .agg(
        F.count("*").alias("cnt"),
        F.min("event_at").alias("first_event_at"),
        F.sum("hours_since_open").alias("sum_hours_since_open_check"),
    )
    .filter(F.col("sum_hours_since_open_check") > 21.6)
    .drop("sum_hours_since_open_check")
)

ranked = (
    grouped
    .select(
        "event_at_ym",
        "agent_id",
        "cnt",
        "first_event_at",
        F.row_number().over(w_all_by_cnt).alias("rn"),
        F.sum("cnt").over(w_all_by_cnt_prior_2).alias("cnt_sum_2_prior"),
        F.rank().over(w_all_by_cnt_ties).alias("cnt_rank"),
    )
)

annotated = (
    ranked
    .filter(F.col("event_at_ym").isNotNull())
    .select(
        "*",
        F.abs(F.col("cnt_sum_2_prior") - 20).alias("cnt_sum_2_prior_distance"),
        F.date_format(F.col("first_event_at"), "MMM yyyy").alias("first_event_at_label"),
    )
)

result = (
    annotated
    .select(
        "event_at_ym",
        "first_event_at",
        "cnt_sum_2_prior",
        "cnt_rank",
        "cnt_sum_2_prior_distance",
        "first_event_at_label",
    )
    .orderBy("event_at_ym", "first_event_at_label")
)
