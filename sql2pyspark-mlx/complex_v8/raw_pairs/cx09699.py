from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_team_csat_by_csat_minu = Window.partitionBy("agent_team", "csat").orderBy("csat_minus_median", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_team_csat_by_csat_minu_2 = Window.partitionBy("agent_team", "csat").orderBy("csat_minus_median", "id")
w_priority_by_avg_csat = Window.partitionBy("priority").orderBy("avg_csat", "id")
w_agent_hired_on_year_agent_te = Window.partitionBy(F.year("agent_hired_on"), "agent_team")

prepared = (
    df_tickets
    .filter((F.col("requester_id") > 7) & (F.col("priority") != "P4"))
    .select(
        "id",
        "agent_id",
        "priority",
        "csat",
        "tags",
        F.date_trunc("year", F.col("closed_at")).alias("closed_at_year"),
        F.concat_ws("-", F.col("priority"), F.col("channel")).alias("priority_channel_label"),
    )
)

joined = (
    prepared.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.priority",
        "s.csat",
        "s.tags",
        "s.closed_at_year",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.team").alias("agent_team"),
    )
)

with_activity = (
    joined.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.event_at") < F.lit("2025-02-18 12:00:00").cast("timestamp")) & (F.col("t.actor") != "system"), "left_anti")
)

priority_stats = (
    with_activity
    .groupBy("priority")
    .agg(F.percentile_approx("csat", 0.5).alias("median_csat"))
)

compared = (
    with_activity.alias("s")
    .join(priority_stats.alias("g"), F.col("g.priority") == F.col("s.priority"), "inner")
    .filter(F.col("s.csat") >= F.col("g.median_csat"))
    .select(
        "s.*",
        "g.median_csat",
        F.round(F.col("s.csat") - F.col("g.median_csat"), 2).alias("csat_minus_median"),
    )
)

filtered = (
    compared.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left_semi")
)

agent_id_stats = (
    filtered
    .groupBy("agent_id")
    .agg(F.avg("csat").alias("avg_csat"))
)

above_average = (
    filtered.alias("s")
    .join(agent_id_stats.alias("g"), F.col("g.agent_id") == F.col("s.agent_id"), "inner")
    .filter(F.col("s.csat") > F.col("g.avg_csat"))
    .select(
        "s.*",
        "g.avg_csat",
        F.round(F.col("s.csat") - F.col("g.avg_csat"), 2).alias("csat_minus_avg"),
    )
)

ranked = (
    above_average
    .select(
        "id",
        "agent_id",
        "csat",
        "closed_at_year",
        "csat_minus_median",
        "avg_csat",
        "csat_minus_avg",
        F.last("avg_csat", ignorenulls=True).over(w_agent_team_csat_by_csat_minu).alias("last_known_avg_csat"),
        F.ntile(4).over(w_agent_team_csat_by_csat_minu_2).alias("csat_minus_median_quartile"),
        (F.col("csat_minus_median") - F.lag("csat_minus_median").over(w_priority_by_avg_csat)).alias("csat_minus_median_change"),
        F.count(F.lit(1)).over(w_agent_hired_on_year_agent_te).alias("rows_in_agent_hired_on_year_agent_team"),
    )
)

result = (
    ranked
    .select(
        "id",
        "csat_minus_median_quartile",
        "rows_in_agent_hired_on_year_agent_team",
    )
    .unpivot(
        ids=["id"],
        values=["csat_minus_median_quartile", "rows_in_agent_hired_on_year_agent_team"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
