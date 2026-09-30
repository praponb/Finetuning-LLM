from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_ticket_csat_ties = Window.orderBy("ticket_csat")
w_event_type_event_at_month = Window.partitionBy("event_type", F.date_trunc("month", "event_at"))
w_ticket_closed_at_to_date_by = Window.partitionBy(F.to_date("ticket_closed_at")).orderBy("event_at", "id").rowsBetween(-6, Window.currentRow)
w_ticket_id_ticket_csat_by_tic = Window.partitionBy("ticket_id", "ticket_csat").orderBy(F.col("ticket_closed_at").desc(), "id")
w_all_by_ticket_closed_at_runn = Window.orderBy(F.col("ticket_closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_csat_actor_by_days_si = Window.partitionBy("ticket_csat", "actor").orderBy(F.col("days_since_event_at").asc_nulls_last(), "id")
w_all_by_ticket_closed_at_movi = Window.orderBy("ticket_closed_at", "id").rowsBetween(-6, Window.currentRow)
w_all_by_ticket_csat = Window.orderBy("ticket_csat", "id")
w_ticket_csat_agent_tier = Window.partitionBy("ticket_csat", "agent_tier")
w_ticket_id_ticket_csat_by_tic_2 = Window.partitionBy("ticket_id", "ticket_csat").orderBy(F.col("ticket_closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_closed_at_to_date_by_2 = Window.partitionBy(F.to_date("ticket_closed_at")).orderBy("event_at")
w_agent_tier_event_at_month = Window.partitionBy("agent_tier", F.month("event_at"))
w_all_by_ticket_csat_running = Window.orderBy("ticket_csat", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_events = (
    df_ticket_events
    .filter(F.col("actor") == "system")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

with_details = (
    scoped_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .select(
        "s.*",
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        "su.agent_name",
        F.col("su.tier").alias("agent_tier"),
    )
)

with_windows = (
    with_details
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_ticket_csat_ties), 3).alias("ticket_csat_cume_dist"),
        F.round(F.col("days_since_event_at") - F.avg("days_since_event_at").over(w_event_type_event_at_month), 2).alias("days_since_event_at_vs_event_type_event_at_month"),
        F.round(F.avg("ticket_csat").over(w_ticket_closed_at_to_date_by), 2).alias("ticket_csat_avg_7"),
        F.lead("ticket_csat").over(w_ticket_id_ticket_csat_by_tic).alias("next_ticket_csat"),
        F.count(F.lit(1)).over(w_all_by_ticket_closed_at_runn).alias("running_count"),
        F.ntile(10).over(w_ticket_csat_actor_by_days_si).alias("days_since_event_at_decile"),
        F.max("ticket_csat").over(w_all_by_ticket_closed_at_movi).alias("ticket_csat_max_7"),
        F.lead("days_since_event_at", 1, 0).over(w_all_by_ticket_csat).alias("next_days_since_event_at"),
        F.sum("ticket_csat").over(w_ticket_csat_agent_tier).alias("ticket_csat_agent_tier_sum_ticket_csat"),
        F.first("ticket_closed_at", ignorenulls=True).over(w_ticket_id_ticket_csat_by_tic_2).alias("first_known_ticket_closed_at"),
        F.round(F.percent_rank().over(w_ticket_closed_at_to_date_by_2), 3).alias("event_at_percent_rank"),
        F.lead("ticket_closed_at").over(w_all_by_ticket_csat).alias("next_ticket_closed_at"),
        F.sum("days_since_event_at").over(w_ticket_csat_agent_tier).alias("ticket_csat_agent_tier_sum_days_since_event_at"),
        F.size(F.collect_set("actor").over(w_agent_tier_event_at_month)).alias("distinct_actor_in_agent_tier_event_at_month"),
        F.sum("ticket_csat").over(w_all_by_ticket_csat_running).alias("running_ticket_csat"),
    )
)

scored = (
    with_windows
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
        F.round((F.unix_timestamp(F.col("ticket_closed_at")) - F.unix_timestamp(F.col("next_ticket_closed_at"))) / 3600, 1).alias("hours_next_ticket_closed_at_to_ticket_closed_at"),
        F.year(F.col("event_at")).alias("event_at_year"),
    )
)

result = (
    scored
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        "days_since_event_at",
        "ticket_closed_at",
        "agent_name",
        "ticket_csat_cume_dist",
        "days_since_event_at_vs_event_type_event_at_month",
        "ticket_csat_avg_7",
        "next_ticket_csat",
        "running_count",
        "days_since_event_at_decile",
        "ticket_csat_max_7",
        "next_days_since_event_at",
        "ticket_csat_agent_tier_sum_ticket_csat",
        "first_known_ticket_closed_at",
        "next_ticket_closed_at",
        "ticket_csat_agent_tier_sum_days_since_event_at",
        "distinct_actor_in_agent_tier_event_at_month",
        "running_ticket_csat",
        "event_type_slug",
        "hours_next_ticket_closed_at_to_ticket_closed_at",
        "event_at_year",
        F.least(F.col("running_ticket_csat"), F.col("ticket_csat_max_7")).alias("least_running_ticket_csat_ticket_csat_max_7"),
        F.date_format(F.col("next_ticket_closed_at"), "MMM yyyy").alias("next_ticket_closed_at_label"),
    )
    .orderBy(F.col("agent_name").desc(), "id")
    .limit(20)
)
