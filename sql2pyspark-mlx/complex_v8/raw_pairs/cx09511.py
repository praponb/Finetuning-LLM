from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_csat_by_closed_at_f = Window.partitionBy("agent_id", "csat").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_csat_full = Window.orderBy(F.col("csat").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_csat_ties = Window.orderBy(F.col("csat").asc_nulls_last())
w_all_by_csat = Window.orderBy(F.col("csat").asc_nulls_last(), "id")
w_first_closed_at_to_date_firs = Window.partitionBy(F.to_date("first_closed_at"), "first_csat_2").orderBy(F.col("first_closed_at").desc(), "id").rowsBetween(-6, Window.currentRow)
w_first_closed_at_to_date_firs_2 = Window.partitionBy(F.to_date("first_closed_at"), "first_csat_2").orderBy(F.col("first_closed_at").desc(), "id")
w_all_by_csat_cume_dist_full = Window.orderBy("csat_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_first_closed_at_to_date_firs_3 = Window.partitionBy(F.to_date("first_closed_at"), "first_csat_2").orderBy(F.col("first_closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_tickets
    .filter((~F.array_contains(F.col("tags"), "bug")) & (F.col("csat") > 2))
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "csat",
    )
)

sequenced = (
    filtered
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "csat",
        F.first("closed_at").over(w_agent_id_csat_by_closed_at_f).alias("first_closed_at"),
        F.first("csat").over(w_all_by_csat_full).alias("first_csat"),
        F.round(F.percent_rank().over(w_all_by_csat_ties), 3).alias("csat_percent_rank"),
        F.round(F.cume_dist().over(w_all_by_csat_ties), 3).alias("csat_cume_dist"),
        F.first("csat").over(w_agent_id_csat_by_closed_at_f).alias("first_csat_2"),
        F.rank().over(w_all_by_csat).alias("csat_rank"),
    )
)

combined_details_agent = (
    sequenced.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.csat",
        "s.first_closed_at",
        "s.csat_percent_rank",
        "s.csat_cume_dist",
        "s.first_csat_2",
        "s.csat_rank",
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

leaders = combined_details_agent.filter(F.col("csat_rank") <= 5)

with_windows = (
    leaders
    .select(
        "*",
        F.round(F.avg("csat_cume_dist").over(w_first_closed_at_to_date_firs), 2).alias("csat_cume_dist_avg_7"),
        F.ntile(5).over(w_first_closed_at_to_date_firs_2).alias("first_closed_at_quintile"),
        F.nth_value("csat_cume_dist", 2).over(w_all_by_csat_cume_dist_full).alias("second_csat_cume_dist"),
        F.round((F.unix_timestamp("first_closed_at") - F.unix_timestamp(F.lag("first_closed_at").over(w_first_closed_at_to_date_firs_2))) / 60, 1).alias("minutes_since_prev_first_closed_at"),
        F.nth_value("first_closed_at", 2).over(w_first_closed_at_to_date_firs_3).alias("second_first_closed_at"),
    )
)

agent_id_stats = (
    with_windows
    .groupBy("agent_id")
    .agg(F.avg("second_csat_cume_dist").alias("avg_second_csat_cume_dist"))
)

outliers = (
    with_windows.alias("s")
    .join(agent_id_stats.alias("g"), F.col("g.agent_id") == F.col("s.agent_id"), "inner")
    .filter(F.col("s.second_csat_cume_dist") > F.col("g.avg_second_csat_cume_dist"))
    .select(
        "s.*",
        "g.avg_second_csat_cume_dist",
        F.round(F.col("s.second_csat_cume_dist") - F.col("g.avg_second_csat_cume_dist"), 2).alias("second_csat_cume_dist_minus_avg"),
    )
)

with_children = (
    outliers.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.event_at") >= F.lit("2025-01-25 09:00:00").cast("timestamp")), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.csat",
        "s.csat_percent_rank",
        "s.second_csat_cume_dist",
        "s.minutes_since_prev_first_closed_at",
        "s.second_first_closed_at",
        "s.avg_second_csat_cume_dist",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        "t.event_type",
        "t.event_at",
    )
)

result = (
    with_children
    .select(
        "id",
        "agent_id",
        "csat",
        "csat_percent_rank",
        "second_csat_cume_dist",
        "minutes_since_prev_first_closed_at",
        "second_first_closed_at",
        "avg_second_csat_cume_dist",
        "event_id",
        "event_type",
        "event_at",
        (F.col("csat_percent_rank") - F.col("second_csat_cume_dist")).alias("csat_percent_rank_minus_second_csat_cume_dist"),
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
    )
    .orderBy(
        F.col("event_type").asc(),
        F.col("csat").desc(),
        "id",
        "event_id",
    )
    .limit(5)
)
