from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_max_ticket_csat_by_least_cnt = Window.partitionBy("max_ticket_csat").orderBy(F.col("least_cnt_distinct_tier_groups").desc(), "max_ticket_csat")
w_max_ticket_csat_by_least_cnt_2 = Window.partitionBy("max_ticket_csat").orderBy(F.col("least_cnt_distinct_tier_groups").desc(), "max_ticket_csat").rowsBetween(-6, -1)
w_max_ticket_csat_by_least_cnt_3 = Window.partitionBy("max_ticket_csat").orderBy(F.col("least_cnt_distinct_tier_groups").desc(), "max_ticket_csat").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_max_ticket_csat_by_least_cnt_4 = Window.partitionBy("max_ticket_csat").orderBy(F.col("least_cnt_distinct_tier_groups").desc(), "max_ticket_csat").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_support_agents
    .filter(F.col("hired_on").between(F.lit("2024-07-09").cast("date"), F.lit("2025-11-09").cast("date")))
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        F.when(F.col("tier").isin("T1", "T3"), "group_a").when(F.col("tier") == "T2", "group_b").otherwise("other").alias("tier_group"),
        F.when(F.col("tier").isin("T2", "T3"), "group_a").when(F.col("tier") == "T1", "group_b").otherwise("other").alias("tier_group_2"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count("*").alias("ticket_count"),
        F.max("csat").alias("max_ticket_csat"),
        F.min("closed_at").alias("first_ticket_closed_at"),
    )
)

profiled = (
    scoped.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
        "t.max_ticket_csat",
        "t.first_ticket_closed_at",
    )
)

detail_rows_tickets = (
    profiled.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.id")) & (F.col("t.requester_id").between(2, 22)), "left")
    .select(
        "s.id",
        "s.agent_name",
        "s.team",
        "s.tier_group",
        "s.tier_group_2",
        "s.max_ticket_csat",
        "s.first_ticket_closed_at",
        F.col("t.id").alias("ticket_id"),
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.requester_id").alias("ticket_requester_id"),
    )
)

rollup_base = (
    detail_rows_tickets
    .filter(F.col("max_ticket_csat") > 3)
    .groupBy("max_ticket_csat")
    .agg(
        F.countDistinct("tier_group").alias("distinct_tier_groups"),
        F.count("*").alias("cnt"),
    )
)

derived = (
    rollup_base
    .select(
        "max_ticket_csat",
        "distinct_tier_groups",
        F.least(F.col("cnt"), F.col("distinct_tier_groups")).alias("least_cnt_distinct_tier_groups"),
    )
)

without_matches = (
    derived
    .crossJoin(derived.agg(F.avg("distinct_tier_groups").alias("overall_avg_distinct_tier_groups")))
    .filter(F.col("distinct_tier_groups") > F.col("overall_avg_distinct_tier_groups"))
    .drop("overall_avg_distinct_tier_groups")
)

metrics = (
    without_matches
    .select(
        "*",
        F.ntile(5).over(w_max_ticket_csat_by_least_cnt).alias("least_cnt_distinct_tier_groups_quintile"),
        F.max("max_ticket_csat").over(w_max_ticket_csat_by_least_cnt_2).alias("max_ticket_csat_max_6_prior"),
        F.last("max_ticket_csat", ignorenulls=True).over(w_max_ticket_csat_by_least_cnt_3).alias("last_known_max_ticket_csat"),
        F.row_number().over(w_max_ticket_csat_by_least_cnt).alias("least_cnt_distinct_tier_groups_seq"),
        F.last("max_ticket_csat").over(w_max_ticket_csat_by_least_cnt_4).alias("last_max_ticket_csat"),
        F.nth_value("max_ticket_csat", 2).over(w_max_ticket_csat_by_least_cnt_4).alias("second_max_ticket_csat"),
    )
)

result = (
    metrics
    .filter(F.col("max_ticket_csat") >= 3)
    .select(
        "max_ticket_csat",
        "least_cnt_distinct_tier_groups_quintile",
        "max_ticket_csat_max_6_prior",
        "last_known_max_ticket_csat",
        "least_cnt_distinct_tier_groups_seq",
        "last_max_ticket_csat",
        F.least(F.col("distinct_tier_groups"), F.col("last_max_ticket_csat")).alias("least_distinct_tier_groups_last_max_ticket_csat"),
        F.round(F.col("second_max_ticket_csat") / F.nullif(F.col("distinct_tier_groups"), F.lit(0)), 3).alias("second_max_ticket_csat_per_distinct_tier_groups"),
    )
    .orderBy(F.col("max_ticket_csat").desc())
)
