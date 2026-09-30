from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_by_hours_since_op = Window.partitionBy("event_type").orderBy(F.col("hours_since_open").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type_by_hours_since_op_2 = Window.partitionBy("event_type").orderBy(F.col("hours_since_open").desc(), "id")
w_ticket_id_event_type = Window.partitionBy("ticket_id", "event_type")

covered = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 2 DAYS"))),
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

joined = (
    covered.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select("s.*", "su.agent_name", F.col("su.team").alias("agent_team"))
)

with_windows = (
    joined
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "agent_id",
        "hours_since_open",
        "agent_name",
        "agent_team",
        F.first("hours_since_open", ignorenulls=True).over(w_event_type_by_hours_since_op).alias("first_known_hours_since_open"),
        F.lag("event_at").over(w_event_type_by_hours_since_op_2).alias("prev_event_at"),
        F.ntile(3).over(w_event_type_by_hours_since_op_2).alias("hours_since_open_tercile"),
        F.sum("hours_since_open").over(w_ticket_id_event_type).alias("ticket_id_event_type_sum_hours_since_open"),
    )
)

rollup_base = (
    with_windows
    .filter(F.col("agent_team") != "Billing")
    .groupBy("ticket_id", F.date_format("event_at", "yyyy-MM").alias("event_at_ym"))
    .agg(
        F.round(100 * F.count(F.when(F.col("agent_team") == "VIP", 1)) / F.count("*"), 1).alias("pct_agent_team_vip"),
        F.array_sort(F.collect_set("agent_team")).alias("agent_team_list"),
        F.array_sort(F.collect_set("event_type")).alias("event_type_list"),
        F.percentile_approx("hours_since_open", 0.5).alias("median_hours_since_open"),
        F.max("hours_since_open").alias("max_hours_since_open_check"),
    )
    .filter(F.col("max_hours_since_open_check") > 18.0)
    .drop("max_hours_since_open_check")
)

result = (
    rollup_base
    .select(
        "ticket_id",
        "event_at_ym",
        "pct_agent_team_vip",
        "median_hours_since_open",
        F.when(F.col("pct_agent_team_vip") >= 60.0, "A").when(F.col("pct_agent_team_vip") >= 30.0, "B").otherwise("C").alias("pct_agent_team_vip_tier"),
    )
    .orderBy(
        F.col("event_at_ym").desc(),
        F.col("pct_agent_team_vip_tier").desc_nulls_first(),
        "ticket_id",
    )
    .limit(20)
)
