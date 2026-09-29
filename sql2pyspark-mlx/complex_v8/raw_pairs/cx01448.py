from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_n_lower_agent_name_li = Window.orderBy("n_lower_agent_name_like_sa", "tier", "last_ticket_closed_at_day").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_n_lower_agent_name_li_2 = Window.orderBy("n_lower_agent_name_like_sa", "tier", "last_ticket_closed_at_day")
w_tier_last_ticket_closed_at_d = Window.partitionBy("tier", F.month("last_ticket_closed_at_day")).orderBy(F.col("max_avg_ticket_requester_id").desc(), "tier", "last_ticket_closed_at_day").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_last_ticket_closed_at_day_mo = Window.partitionBy(F.date_trunc("month", "last_ticket_closed_at_day"), "n_lower_agent_name_like_sa").orderBy("n_lower_agent_name_like_sa", "tier", "last_ticket_closed_at_day")

recent_agents = (
    df_support_agents
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
        F.year(F.col("hired_on")).alias("hired_on_year"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.round(F.avg("requester_id"), 2).alias("avg_ticket_requester_id"),
        F.max("closed_at").alias("last_ticket_closed_at"),
        F.max("csat").alias("max_ticket_csat"),
    )
)

with_stats = (
    recent_agents.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "t.avg_ticket_requester_id",
        "t.last_ticket_closed_at",
        "t.max_ticket_csat",
    )
)

with_activity = (
    with_stats.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.id")) & (F.month(F.col("t.opened_at")).isin(1, 2)), "left_semi")
    .filter(F.col("hired_on").between(F.lit("2024-04-14").cast("date"), F.lit("2025-03-30").cast("date")))
)

totals = (
    with_activity
    .groupBy(
        "tier",
        F.to_date("last_ticket_closed_at").alias("last_ticket_closed_at_day"),
    )
    .agg(
        F.max("avg_ticket_requester_id").alias("max_avg_ticket_requester_id"),
        F.sum(F.when(F.lower(F.col("agent_name")).like("%sa%"), 1).otherwise(0)).alias("n_lower_agent_name_like_sa"),
        F.max("hired_on_year").alias("max_hired_on_year_check"),
    )
    .filter(F.col("max_hired_on_year_check") > 2025)
    .drop("max_hired_on_year_check")
)

with_trends = (
    totals
    .select(
        "tier",
        "last_ticket_closed_at_day",
        F.count(F.lit(1)).over(w_all_by_n_lower_agent_name_li).alias("running_count"),
        (F.col("n_lower_agent_name_like_sa") - F.lag("n_lower_agent_name_like_sa").over(w_all_by_n_lower_agent_name_li_2)).alias("n_lower_agent_name_like_sa_change"),
        F.sum("max_avg_ticket_requester_id").over(w_tier_last_ticket_closed_at_d).alias("running_max_avg_ticket_requester_id"),
        F.lag("last_ticket_closed_at_day", 3).over(w_last_ticket_closed_at_day_mo).alias("prev3_last_ticket_closed_at_day"),
    )
)

result = (
    with_trends
    .select(
        "tier",
        "last_ticket_closed_at_day",
        "running_count",
        "n_lower_agent_name_like_sa_change",
        "running_max_avg_ticket_requester_id",
        "prev3_last_ticket_closed_at_day",
        F.coalesce(F.col("tier"), F.lit("UNKNOWN")).alias("tier_or_unknown"),
        F.abs(F.col("running_count") - 10).alias("running_count_distance"),
    )
    .orderBy("running_count_distance")
)
