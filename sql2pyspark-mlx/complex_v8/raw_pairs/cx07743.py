from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_opened_at_to_date_prior_rows = Window.partitionBy(F.to_date("opened_at"), "prior_rows_12h").orderBy(F.col("prior_csat_12h").desc())
w_agent_id_agent_tier = Window.partitionBy("agent_id", "agent_tier")
w_all_by_prior_rows_12h = Window.orderBy(F.col("prior_rows_12h").desc(), "id")

matched = (
    df_tickets.alias("a")
    .join(
        df_tickets.alias("b"),
        (F.col("b.agent_id") == F.col("a.agent_id"))
        & (F.col("b.opened_at") < F.col("a.opened_at"))
        & (F.col("b.opened_at") >= F.col("a.opened_at") - F.expr("INTERVAL 12 HOURS")),
        "left",
    )
    .groupBy("a.id", "a.agent_id", "a.opened_at")
    .agg(
        F.count("b.id").alias("prior_rows_12h"),
        F.sum("b.csat").alias("prior_csat_12h"),
    )
)

with_details = (
    matched.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.*",
        F.col("su.tier").alias("agent_tier"),
        "su.agent_name",
        F.col("su.team").alias("agent_team"),
    )
)

windowed = (
    with_details
    .select(
        "*",
        F.rank().over(w_opened_at_to_date_prior_rows).alias("prior_csat_12h_rank"),
        F.round(F.col("prior_csat_12h") - F.avg("prior_csat_12h").over(w_agent_id_agent_tier), 2).alias("prior_csat_12h_vs_agent_id_agent_tier_avg"),
        F.lag("prior_csat_12h").over(w_all_by_prior_rows_12h).alias("prev_prior_csat_12h"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.max("closed_at").alias("last_ticket_closed_at"),
        F.max("closed_at").alias("last_ticket_closed_at_2"),
    )
)

profiled = (
    windowed.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select("s.*", "t.last_ticket_closed_at", "t.last_ticket_closed_at_2")
)

result = (
    profiled
    .groupingSets(
        [["agent_team", "agent_id"], ["agent_team"], ["agent_id"]],
        "agent_team", "agent_id",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.count("*").alias("cnt"),
        F.round(F.stddev("prior_rows_12h"), 2).alias("stddev_prior_rows_12h"),
    )
    .orderBy(
        F.col("agent_team").asc_nulls_last(),
        F.col("agent_id").asc_nulls_last(),
    )
)
