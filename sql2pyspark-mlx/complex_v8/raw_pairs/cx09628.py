from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type = Window.partitionBy("event_type")
w_ticket_id_event_at_year_by_e = Window.partitionBy("ticket_id", "event_at_year").orderBy(F.col("event_at").desc_nulls_first(), "id")
w_all_by_event_at_year = Window.orderBy("event_at_year", "id")
w_event_at_year_by_event_at_ru = Window.partitionBy("event_at_year").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_agent_team = Window.partitionBy("ticket_id", "agent_team")
w_agent_team_actor_by_event_at = Window.partitionBy("agent_team", "actor").orderBy(F.col("event_at").desc(), "id")
w_all_by_ticket_requester_id_c = Window.orderBy("ticket_requester_id_change", "id")
w_all_by_ticket_requester_id_c_2 = Window.orderBy("ticket_requester_id_change")
w_event_at_year = Window.partitionBy("event_at_year")
w_event_at_year_by_event_at_ye = Window.partitionBy("event_at_year").orderBy("event_at_year_seq", "id").rowsBetween(-6, -1)

valid_events = (
    df_ticket_events
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
        F.year(F.col("event_at")).alias("event_at_year"),
    )
)

joined = (
    valid_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .filter(F.col("t.requester_id") <= 22)
    .select(
        "s.*",
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
    )
)

metrics = (
    joined
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "event_at_year",
        "ticket_tags",
        "ticket_requester_id",
        "agent_tier",
        "agent_team",
        F.round(F.avg("ticket_requester_id").over(w_event_type), 2).alias("event_type_avg_ticket_requester_id"),
        F.lag("ticket_requester_id").over(w_ticket_id_event_at_year_by_e).alias("prev_ticket_requester_id"),
        F.dense_rank().over(w_ticket_id_event_at_year_by_e).alias("event_at_dense_rank"),
        F.rank().over(w_all_by_event_at_year).alias("event_at_year_rank"),
        F.first("event_at", ignorenulls=True).over(w_event_at_year_by_event_at_ru).alias("first_known_event_at"),
        (F.col("ticket_requester_id") - F.lag("ticket_requester_id").over(w_ticket_id_event_at_year_by_e)).alias("ticket_requester_id_change"),
        F.count(F.lit(1)).over(w_ticket_id_agent_team).alias("rows_in_ticket_id_agent_team"),
        F.lead("ticket_requester_id", 1, 0).over(w_agent_team_actor_by_event_at).alias("next_ticket_requester_id"),
        F.row_number().over(w_all_by_event_at_year).alias("event_at_year_seq"),
    )
)

leaders = (
    metrics
    .filter(F.col("event_at_year_rank") <= 2)
    .drop("event_at_year_rank")
)

with_windows = (
    leaders
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "ticket_tags",
        "ticket_requester_id",
        "agent_team",
        "event_type_avg_ticket_requester_id",
        "prev_ticket_requester_id",
        "event_at_dense_rank",
        "ticket_requester_id_change",
        "rows_in_ticket_id_agent_team",
        "next_ticket_requester_id",
        "event_at_year_seq",
        F.row_number().over(w_all_by_ticket_requester_id_c).alias("ticket_requester_id_change_seq"),
        F.dense_rank().over(w_all_by_ticket_requester_id_c_2).alias("ticket_requester_id_change_dense_rank"),
        F.size(F.collect_set("actor").over(w_event_at_year)).alias("distinct_actor_in_event_at_year"),
        F.lead("next_ticket_requester_id").over(w_all_by_ticket_requester_id_c).alias("next_next_ticket_requester_id"),
        F.max("prev_ticket_requester_id").over(w_event_at_year_by_event_at_ye).alias("prev_ticket_requester_id_max_6_prior"),
        F.round(F.col("event_at_year") - F.avg("event_at_year").over(w_event_at_year), 2).alias("event_at_year_vs_event_at_year_avg"),
        (F.col("prev_ticket_requester_id") - F.lag("prev_ticket_requester_id").over(w_all_by_ticket_requester_id_c)).alias("prev_ticket_requester_id_change"),
        F.count(F.lit(1)).over(w_event_at_year).alias("rows_in_event_at_year"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "actor",
        "ticket_tags",
        "agent_team",
        "event_type_avg_ticket_requester_id",
        "prev_ticket_requester_id",
        "ticket_requester_id_change",
        "next_ticket_requester_id",
        "event_at_year_seq",
        "ticket_requester_id_change_seq",
        "ticket_requester_id_change_dense_rank",
        "distinct_actor_in_event_at_year",
        "next_next_ticket_requester_id",
        "event_at_year_vs_event_at_year_avg",
        "prev_ticket_requester_id_change",
        "rows_in_event_at_year",
        F.date_trunc("hour", F.col("event_at")).alias("event_at_hour"),
        F.to_date(F.col("event_at")).alias("event_at_date"),
    )
    .orderBy("ticket_requester_id_change", "id")
    .limit(10)
)
