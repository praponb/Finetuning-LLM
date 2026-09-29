from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_prior_rows_1d_agent_id = Window.partitionBy("prior_rows_1d", "agent_id")
w_prior_rows_1d_agent_id_by_pr = Window.partitionBy("prior_rows_1d", "agent_id").orderBy("prior_rows_1d")

matched = (
    df_tickets.alias("a")
    .join(
        df_tickets.alias("b"),
        (F.col("b.agent_id") == F.col("a.agent_id"))
        & (F.col("b.opened_at") < F.col("a.opened_at"))
        & (F.col("b.opened_at") >= F.col("a.opened_at") - F.expr("INTERVAL 1 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.agent_id", "a.opened_at")
    .agg(
        F.count("b.id").alias("prior_rows_1d"),
        F.sum("b.csat").alias("prior_csat_1d"),
    )
)

with_trends = (
    matched
    .select(
        "id",
        "agent_id",
        "opened_at",
        "prior_rows_1d",
        "prior_csat_1d",
        F.round(F.avg("prior_rows_1d").over(w_prior_rows_1d_agent_id), 2).alias("prior_rows_1d_agent_id_avg_prior_rows_1d"),
        F.round(F.cume_dist().over(w_prior_rows_1d_agent_id_by_pr), 3).alias("prior_rows_1d_cume_dist"),
    )
)

enriched = (
    with_trends
    .filter(F.col("agent_id").isNotNull())
    .select(
        "*",
        F.round(F.col("prior_csat_1d") / F.nullif(F.col("prior_rows_1d_cume_dist"), F.lit(0)), 3).alias("prior_csat_1d_per_prior_rows_1d_cume_dist"),
        F.coalesce(F.col("prior_rows_1d"), F.lit(0)).alias("prior_rows_1d_filled"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count("*").alias("event_count"),
        F.min("event_at").alias("first_event_at"),
    )
)

profiled = (
    enriched.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        "t.first_event_at",
    )
)

result = (
    profiled
    .groupingSets(
        [["agent_id", "event_count", "prior_rows_1d_filled"], ["agent_id"], []],
        "agent_id", "event_count", "prior_rows_1d_filled",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.round(F.avg("prior_csat_1d_per_prior_rows_1d_cume_dist"), 2).alias("avg_prior_csat_1d_per_prior_rows_1d_cume_dist"),
        F.percentile_approx("prior_rows_1d_cume_dist", 0.5).alias("median_prior_rows_1d_cume_dist"),
    )
)
