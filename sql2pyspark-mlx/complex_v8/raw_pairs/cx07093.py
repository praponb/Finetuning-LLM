from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_ticket_id = Window.partitionBy("event_type", "ticket_id")
w_all_by_event_at_prior_1 = Window.orderBy("event_at", "id").rowsBetween(-1, -1)
w_event_type_by_event_at_ties = Window.partitionBy("event_type").orderBy("event_at")
w_ticket_agent_id_event_type_b = Window.partitionBy("ticket_agent_id", "event_type").orderBy(F.col("ticket_csat_max_1_prior").desc())
w_event_type_by_event_at_full = Window.partitionBy("event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_by_event_at_movin = Window.partitionBy("event_type").orderBy("event_at", "id").rowsBetween(-6, Window.currentRow)
w_ticket_id_ticket_csat = Window.partitionBy("ticket_id", "ticket_csat")

filtered_events = (
    df_ticket_events
    .filter(F.col("event_type") == "created")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_parent_ticket = (
    filtered_events.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.month(F.col("t.opened_at")).isin(1, 3)), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.tags").alias("ticket_tags"),
    )
)

combined_details = (
    with_parent_ticket.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.requester_id").between(5, 18)), "inner")
    .select("s.*", F.col("t.agent_id").alias("ticket_agent_id"))
)

windowed = (
    combined_details
    .select(
        "*",
        F.count(F.lit(1)).over(w_event_type_ticket_id).alias("rows_in_event_type_ticket_id"),
        F.max("ticket_csat").over(w_all_by_event_at_prior_1).alias("ticket_csat_max_1_prior"),
    )
)

windowed_2 = (
    windowed
    .select(
        "id",
        "event_at",
        "ticket_csat",
        "ticket_tags",
        "rows_in_event_type_ticket_id",
        F.rank().over(w_event_type_by_event_at_ties).alias("event_at_rank"),
        F.round(F.percent_rank().over(w_ticket_agent_id_event_type_b), 3).alias("ticket_csat_max_1_prior_percent_rank"),
        F.nth_value("ticket_csat_max_1_prior", 2).over(w_event_type_by_event_at_full).alias("second_ticket_csat_max_1_prior"),
        F.round(F.avg("ticket_csat").over(w_event_type_by_event_at_movin), 2).alias("ticket_csat_avg_7"),
        F.round(F.col("ticket_csat") / F.nullif(F.sum("ticket_csat").over(w_ticket_id_ticket_csat), F.lit(0)), 4).alias("ticket_csat_share_of_ticket_id_ticket_csat"),
    )
)

result = (
    windowed_2
    .filter(F.col("ticket_csat_avg_7") < 3.8)
    .select(
        "id",
        "event_at",
        "rows_in_event_type_ticket_id",
        "event_at_rank",
        "ticket_csat_max_1_prior_percent_rank",
        "second_ticket_csat_max_1_prior",
        "ticket_csat_avg_7",
        F.to_date(F.col("event_at")).alias("event_at_date"),
        F.date_trunc("month", F.col("event_at")).alias("event_at_month"),
    )
    .orderBy(F.col("second_ticket_csat_max_1_prior").asc_nulls_last())
)
