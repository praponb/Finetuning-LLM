from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_at_month_actor_len_2 = Window.partitionBy(F.month("event_at"), "actor_len_2")
w_all_by_actor_len_2 = Window.orderBy("actor_len_2", "id")
w_actor_len_actor_by_rn_prior = Window.partitionBy("actor_len", "actor").orderBy("rn", "id").rowsBetween(-2, -1)
w_actor_len_actor_by_rn_full = Window.partitionBy("actor_len", "actor").orderBy("rn", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_len_actor_by_rn_ties = Window.partitionBy("actor_len", "actor").orderBy("rn")
w_all_by_event_at_month_actor = Window.orderBy("event_at_month_actor_len_2_max_actor_len_2", "id")
w_all_by_rows_in_event_at_mont = Window.orderBy("rows_in_event_at_month_actor_len_2")
w_event_at_year = Window.partitionBy(F.year("event_at"))

prepared_events = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) | (F.col("event_at") >= F.lit("2025-01-25 09:00:00").cast("timestamp")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
        F.length(F.trim(F.col("actor"))).alias("actor_len_2"),
        F.concat_ws("-", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
    )
)

windowed = (
    prepared_events
    .select(
        "*",
        F.max("actor_len_2").over(w_event_at_month_actor_len_2).alias("event_at_month_actor_len_2_max_actor_len_2"),
        F.row_number().over(w_all_by_actor_len_2).alias("rn"),
        F.count(F.lit(1)).over(w_event_at_month_actor_len_2).alias("rows_in_event_at_month_actor_len_2"),
    )
)

first_rows = windowed.filter(F.col("rn") <= 5)

ranked = (
    first_rows
    .select(
        "*",
        F.sum("rn").over(w_actor_len_actor_by_rn_prior).alias("rn_sum_2_prior"),
        F.last("actor_len").over(w_actor_len_actor_by_rn_full).alias("last_actor_len"),
        F.round(F.percent_rank().over(w_actor_len_actor_by_rn_ties), 3).alias("rn_percent_rank"),
    )
)

sequenced = (
    ranked
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor_len",
        "event_type_actor_label",
        "event_at_month_actor_len_2_max_actor_len_2",
        "rn",
        "rows_in_event_at_month_actor_len_2",
        "rn_sum_2_prior",
        "last_actor_len",
        "rn_percent_rank",
        F.ntile(4).over(w_all_by_event_at_month_actor).alias("event_at_month_actor_len_2_max_actor_len_2_quart"),
        F.rank().over(w_all_by_rows_in_event_at_mont).alias("rows_in_event_at_month_actor_len_2_rank"),
        F.round(F.col("rn") - F.avg("rn").over(w_event_at_year), 2).alias("rn_vs_event_at_year_avg"),
    )
)

with_details_ticket = (
    sequenced.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor_len",
        "s.event_type_actor_label",
        "s.event_at_month_actor_len_2_max_actor_len_2",
        "s.rn",
        "s.rows_in_event_at_month_actor_len_2",
        "s.last_actor_len",
        "s.rn_percent_rank",
        "s.event_at_month_actor_len_2_max_actor_len_2_quart",
        "s.rows_in_event_at_month_actor_len_2_rank",
        "s.rn_vs_event_at_year_avg",
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

line_level = (
    with_details_ticket.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type").isNotNull()), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.event_at_month_actor_len_2_max_actor_len_2",
        "s.rn_percent_rank",
        "s.rn_vs_event_at_year_avg",
        "s.ticket_channel",
        "s.ticket_tags",
        "s.ticket_opened_at",
        "s.agent_team",
        F.col("t.id").alias("event_id"),
        F.col("t.event_type").alias("event_event_type"),
        "t.event_at",
    )
)

result = (
    line_level
    .select(
        "id",
        "ticket_id",
        "event_type",
        "event_at_month_actor_len_2_max_actor_len_2",
        "rn_percent_rank",
        "rn_vs_event_at_year_avg",
        "ticket_channel",
        "ticket_tags",
        "ticket_opened_at",
        "agent_team",
        "event_event_type",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("ticket_opened_at")).alias("days_since_ticket_opened_at"),
        F.when(F.col("event_type").isin("assigned", "reopened"), "group_a").when(F.col("event_type") == "escalated", "group_b").otherwise("other").alias("event_type_group"),
    )
    .orderBy(F.col("id").asc(), "event_at_month_actor_len_2_max_actor_len_2")
)
