from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id")
w_opened_at_to_date_prior_rows = Window.partitionBy(F.to_date("opened_at"), "prior_rows_1d").orderBy("opened_at", "id")
w_opened_at_to_date_prior_rows_2 = Window.partitionBy(F.to_date("opened_at"), "prior_rows_1d").orderBy("opened_at")
w_opened_at_to_date_prior_rows_3 = Window.partitionBy(F.to_date("opened_at"), "prior_rows_1d").orderBy("opened_at", "id").rowsBetween(-3, Window.currentRow)
w_opened_at_to_date_prior_rows_4 = Window.partitionBy(F.to_date("opened_at"), "prior_rows_1d").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

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

with_windows = (
    matched
    .select(
        "*",
        F.round(F.col("prior_rows_1d") - F.avg("prior_rows_1d").over(w_agent_id), 2).alias("prior_rows_1d_vs_agent_id_avg"),
        (F.col("prior_rows_1d") - F.lag("prior_rows_1d").over(w_opened_at_to_date_prior_rows)).alias("prior_rows_1d_change"),
        F.dense_rank().over(w_opened_at_to_date_prior_rows_2).alias("opened_at_dense_rank"),
        F.round(F.avg("prior_csat_1d").over(w_opened_at_to_date_prior_rows_3), 2).alias("prior_csat_1d_avg_4"),
        F.last("opened_at", ignorenulls=True).over(w_opened_at_to_date_prior_rows_4).alias("last_known_opened_at"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(F.round(F.avg("requester_id"), 2).alias("avg_ticket_requester_id"))
)

with_counts = (
    with_windows.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select("s.*", "t.avg_ticket_requester_id")
)

annotated = (
    with_counts
    .filter(F.col("opened_at_dense_rank") <= 6)
    .select(
        "id",
        "agent_id",
        "prior_rows_1d",
        "prior_rows_1d_vs_agent_id_avg",
        "prior_rows_1d_change",
        "opened_at_dense_rank",
        "last_known_opened_at",
        "avg_ticket_requester_id",
        F.when(F.col("prior_csat_1d_avg_4") >= 21.2, "HIGH").when(F.col("prior_csat_1d_avg_4") >= 8.8, "MEDIUM").otherwise("LOW").alias("prior_csat_1d_avg_4_tier"),
    )
)

result = (
    annotated
    .select(
        "id",
        F.col("prior_rows_1d_vs_agent_id_avg").alias("m_prior_rows_1d_vs_agent_id_avg"),
        F.col("avg_ticket_requester_id").alias("avg_ticket_requester_id_v"),
    )
    .unpivot(
        ids=["id"],
        values=["m_prior_rows_1d_vs_agent_id_avg", "avg_ticket_requester_id_v"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "metric_name")
)
