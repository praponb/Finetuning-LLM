from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_agent_id_by_event_at = Window.partitionBy("ticket_agent_id").orderBy(F.col("event_at").desc(), "id")
w_all_by_agent_hired_on_ties = Window.orderBy(F.col("agent_hired_on").desc())
w_ticket_priority_by_agent_hir = Window.partitionBy("ticket_priority").orderBy("agent_hired_on", "id")
w_ticket_agent_id_by_agent_hir = Window.partitionBy("ticket_agent_id").orderBy(F.col("agent_hired_on").desc(), "id")
w_ticket_agent_id_actor_by_eve = Window.partitionBy("ticket_agent_id", "actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_agent_hired_on = Window.orderBy(F.col("agent_hired_on").desc(), "id")
w_all_by_agent_hired_on_runnin = Window.orderBy(F.col("agent_hired_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_agent_id_by_event_at_2 = Window.partitionBy("ticket_agent_id").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_agent_id_actor_by_eve_2 = Window.partitionBy("ticket_agent_id", "actor").orderBy("event_at", "id")
w_last_known_event_at_month_mo = Window.partitionBy(F.date_trunc("month", "last_known_event_at_month"), "distinct_ticket_prioritys").orderBy("max_seq_no", "agent_team", "last_known_event_at_month", "actor")
w_distinct_ticket_prioritys_by = Window.partitionBy("distinct_ticket_prioritys").orderBy("max_seq_no", "agent_team", "last_known_event_at_month", "actor").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_ticket_events
    .filter((~F.col("event_type").isin("created", "resolved")) | (~F.col("actor").isin("customer", "system")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.dayofweek(F.col("event_at")).alias("event_at_dayofweek"),
    )
)

joined = (
    prepared.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

windowed = (
    joined
    .select(
        "*",
        F.row_number().over(w_ticket_agent_id_by_event_at).alias("seq_no"),
        F.round(F.percent_rank().over(w_all_by_agent_hired_on_ties), 3).alias("agent_hired_on_percent_rank"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_ticket_priority_by_agent_hir))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.round(F.cume_dist().over(w_ticket_agent_id_by_agent_hir), 3).alias("agent_hired_on_cume_dist"),
        F.row_number().over(w_ticket_agent_id_by_agent_hir).alias("rn"),
        F.lead("event_at", 2).over(w_ticket_agent_id_by_agent_hir).alias("next2_event_at"),
        F.last("event_at").over(w_ticket_agent_id_actor_by_eve).alias("last_event_at"),
        F.lead("event_at", 2).over(w_all_by_agent_hired_on).alias("next2_event_at_2"),
        F.last("event_at", ignorenulls=True).over(w_all_by_agent_hired_on_runnin).alias("last_known_event_at"),
        F.nth_value("event_at", 2).over(w_ticket_agent_id_by_event_at_2).alias("second_event_at"),
        F.lag("event_at").over(w_ticket_agent_id_by_agent_hir).alias("prev_event_at"),
        F.row_number().over(w_ticket_agent_id_actor_by_eve_2).alias("seq_no_2"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_ticket_agent_id_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at_2"),
    )
)

stats = (
    windowed
    .groupBy(
        "agent_team",
        F.date_trunc("month", "last_known_event_at").alias("last_known_event_at_month"),
        "actor",
    )
    .agg(
        F.max("seq_no").alias("max_seq_no"),
        F.array_sort(F.collect_set("agent_team")).alias("agent_team_list"),
        F.countDistinct("ticket_priority").alias("distinct_ticket_prioritys"),
        F.round(F.avg("agent_hired_on_percent_rank"), 2).alias("avg_agent_hired_on_percent_rank"),
        F.max("agent_hired_on_percent_rank").alias("max_agent_hired_on_percent_rank_check"),
    )
    .filter(F.col("max_agent_hired_on_percent_rank_check") > 0.1)
    .drop("max_agent_hired_on_percent_rank_check")
)

with_windows = (
    stats
    .select(
        "agent_team",
        "last_known_event_at_month",
        "actor",
        "max_seq_no",
        "agent_team_list",
        "distinct_ticket_prioritys",
        "avg_agent_hired_on_percent_rank",
        F.rank().over(w_last_known_event_at_month_mo).alias("max_seq_no_rank"),
        F.first("distinct_ticket_prioritys", ignorenulls=True).over(w_distinct_ticket_prioritys_by).alias("first_known_distinct_ticket_prioritys"),
        F.ntile(3).over(w_last_known_event_at_month_mo).alias("max_seq_no_tercile"),
    )
)

result = (
    with_windows
    .filter(F.col("actor") == "agent")
    .select(
        "agent_team",
        "last_known_event_at_month",
        "max_seq_no",
        "distinct_ticket_prioritys",
        "avg_agent_hired_on_percent_rank",
        "max_seq_no_rank",
        "first_known_distinct_ticket_prioritys",
        "max_seq_no_tercile",
        (F.floor(F.col("max_seq_no_rank") / 2) * 2).alias("max_seq_no_rank_bucket"),
    )
    .orderBy(F.col("agent_team").asc(), "max_seq_no_rank_bucket")
)
