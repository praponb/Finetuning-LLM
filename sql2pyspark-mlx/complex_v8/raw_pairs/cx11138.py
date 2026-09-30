from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tier_or_unknown_by_first_tic = Window.partitionBy("tier_or_unknown").orderBy("first_ticket_closed_at", "id")
w_tier_or_unknown_by_first_tic_2 = Window.partitionBy("tier_or_unknown").orderBy("first_ticket_closed_at")
w_tier_or_unknown_by_first_tic_3 = Window.partitionBy("tier_or_unknown").orderBy("first_ticket_closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_second_avg_ticket_csa = Window.orderBy("second_avg_ticket_csat", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_second_avg_ticket_csa_2 = Window.orderBy("second_avg_ticket_csat")
w_first_ticket_closed_at_rank = Window.partitionBy("first_ticket_closed_at_rank")
w_all_by_second_avg_ticket_csa_3 = Window.orderBy("second_avg_ticket_csat", "id")
w_second_avg_ticket_csat_rank = Window.partitionBy("second_avg_ticket_csat_rank", "ticket_priority_count").orderBy(F.col("first_ticket_closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_first_ticket_closed_at_to_da = Window.partitionBy(F.to_date("first_ticket_closed_at"), "tier_or_unknown")
w_second_avg_ticket_csat_rank_2 = Window.partitionBy("second_avg_ticket_csat_rank", "ticket_priority_count").orderBy(F.col("first_ticket_closed_at").desc(), "id").rowsBetween(-6, Window.currentRow)
w_second_avg_ticket_csat_rank_3 = Window.partitionBy("second_avg_ticket_csat_rank", "ticket_priority_count").orderBy(F.col("first_ticket_closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_agents = (
    df_support_agents
    .filter((~F.col("tier").isin("T1", "T2")) & (F.year(F.col("hired_on")) == 2025))
    .select(
        "id",
        "agent_name",
        "tier",
        "hired_on",
        F.coalesce(F.col("tier"), F.lit("UNKNOWN")).alias("tier_or_unknown"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.round(F.avg("csat"), 2).alias("avg_ticket_csat"),
        F.count(F.when(F.col("priority").isin("P3", "P1"), 1)).alias("ticket_priority_count"),
        F.min("closed_at").alias("first_ticket_closed_at"),
    )
)

profiled = (
    scoped_agents.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "t.avg_ticket_csat",
        F.coalesce(F.col("t.ticket_priority_count"), F.lit(0)).alias("ticket_priority_count"),
        "t.first_ticket_closed_at",
    )
)

windowed = (
    profiled
    .select(
        "*",
        F.ntile(3).over(w_tier_or_unknown_by_first_tic).alias("first_ticket_closed_at_tercile"),
        F.rank().over(w_tier_or_unknown_by_first_tic_2).alias("first_ticket_closed_at_rank"),
        F.nth_value("avg_ticket_csat", 2).over(w_tier_or_unknown_by_first_tic_3).alias("second_avg_ticket_csat"),
        F.lead("hired_on").over(w_tier_or_unknown_by_first_tic).alias("next_hired_on"),
        F.lead("first_ticket_closed_at").over(w_tier_or_unknown_by_first_tic).alias("next_first_ticket_closed_at"),
    )
)

metrics = (
    windowed
    .select(
        "id",
        "agent_name",
        "tier",
        "hired_on",
        "tier_or_unknown",
        "avg_ticket_csat",
        "ticket_priority_count",
        "first_ticket_closed_at",
        "first_ticket_closed_at_tercile",
        "first_ticket_closed_at_rank",
        "second_avg_ticket_csat",
        "next_first_ticket_closed_at",
        F.last("avg_ticket_csat", ignorenulls=True).over(w_all_by_second_avg_ticket_csa).alias("last_known_avg_ticket_csat"),
        F.rank().over(w_all_by_second_avg_ticket_csa_2).alias("second_avg_ticket_csat_rank"),
        F.round(F.cume_dist().over(w_all_by_second_avg_ticket_csa_2), 3).alias("second_avg_ticket_csat_cume_dist"),
        F.round(F.col("second_avg_ticket_csat") / F.nullif(F.sum("second_avg_ticket_csat").over(w_first_ticket_closed_at_rank), F.lit(0)), 4).alias("second_avg_ticket_csat_share_of_first_ticket_clo"),
        F.round(F.col("avg_ticket_csat") - F.avg("avg_ticket_csat").over(w_first_ticket_closed_at_rank), 2).alias("avg_ticket_csat_vs_first_ticket_closed_at_rank_a"),
        F.dense_rank().over(w_all_by_second_avg_ticket_csa_3).alias("second_avg_ticket_csat_dense_rank"),
    )
)

metrics_2 = (
    metrics
    .select(
        "*",
        F.nth_value("second_avg_ticket_csat_rank", 2).over(w_second_avg_ticket_csat_rank).alias("second_second_avg_ticket_csat_rank"),
        F.count(F.lit(1)).over(w_first_ticket_closed_at_to_da).alias("rows_in_first_ticket_closed_at_to_date_tier_or_u"),
        F.round(F.avg("ticket_priority_count").over(w_second_avg_ticket_csat_rank_2), 2).alias("ticket_priority_count_avg_7"),
        F.nth_value("hired_on", 2).over(w_second_avg_ticket_csat_rank).alias("second_hired_on"),
        F.last("ticket_priority_count", ignorenulls=True).over(w_second_avg_ticket_csat_rank_3).alias("last_known_ticket_priority_count"),
    )
)

line_level_tickets = (
    metrics_2.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.agent_name",
        "s.hired_on",
        "s.avg_ticket_csat",
        "s.first_ticket_closed_at",
        "s.first_ticket_closed_at_tercile",
        "s.second_avg_ticket_csat_rank",
        "s.second_avg_ticket_csat_share_of_first_ticket_clo",
        "s.rows_in_first_ticket_closed_at_to_date_tier_or_u",
        "s.ticket_priority_count_avg_7",
        "s.second_hired_on",
        F.col("t.id").alias("ticket_id"),
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.tags").alias("ticket_tags"),
    )
)

result = (
    line_level_tickets
    .filter(F.col("ticket_priority_count_avg_7") < 6.4)
    .select(
        "id",
        "avg_ticket_csat",
        "first_ticket_closed_at",
        "first_ticket_closed_at_tercile",
        "second_avg_ticket_csat_rank",
        "second_avg_ticket_csat_share_of_first_ticket_clo",
        "rows_in_first_ticket_closed_at_to_date_tier_or_u",
        "ticket_priority_count_avg_7",
        "ticket_id",
        "ticket_csat",
    )
    .orderBy(
        "rows_in_first_ticket_closed_at_to_date_tier_or_u",
        F.col("second_avg_ticket_csat_rank").desc(),
    )
)
