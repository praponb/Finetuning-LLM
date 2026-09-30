from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_by_opened_at = Window.partitionBy("agent_id").orderBy(F.col("opened_at").desc(), "id")
w_agent_id_by_prior_rows_1d_ti = Window.partitionBy("agent_id").orderBy("prior_rows_1d")
w_prior_rows_1d = Window.partitionBy("prior_rows_1d")

covered = (
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
    covered
    .select(
        "*",
        F.lead("opened_at", 2).over(w_agent_id_by_opened_at).alias("next2_opened_at"),
        F.round(F.cume_dist().over(w_agent_id_by_prior_rows_1d_ti), 3).alias("prior_rows_1d_cume_dist"),
        F.size(F.collect_set("agent_id").over(w_prior_rows_1d)).alias("distinct_agent_id_in_prior_rows_1d"),
    )
)

qualified = (
    with_trends
    .crossJoin(with_trends.agg(F.max("prior_rows_1d_cume_dist").alias("overall_max_prior_rows_1d_cume_dist")))
    .filter(F.col("prior_rows_1d_cume_dist") < F.col("overall_max_prior_rows_1d_cume_dist"))
    .drop("overall_max_prior_rows_1d_cume_dist")
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.min("event_at").alias("first_event_at"))
)

with_counts = (
    qualified.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select("s.*", "t.first_event_at")
)

result = (
    with_counts
    .select(
        "id",
        F.col("prior_csat_1d").alias("m_prior_csat_1d"),
        F.col("prior_rows_1d").alias("m_prior_rows_1d"),
        F.col("distinct_agent_id_in_prior_rows_1d").alias("m_distinct_agent_id_in_prior_rows_1d"),
    )
    .unpivot(
        ids=["id"],
        values=["m_prior_csat_1d", "m_prior_rows_1d", "m_distinct_agent_id_in_prior_rows_1d"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
)
