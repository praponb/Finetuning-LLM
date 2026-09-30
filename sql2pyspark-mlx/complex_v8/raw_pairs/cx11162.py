from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_opened_at_to_date_agent_id = Window.partitionBy(F.to_date("opened_at"), "agent_id")
w_all_by_opened_at_full = Window.orderBy(F.col("opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_opened_at = Window.orderBy(F.col("opened_at").desc(), "id")
w_prior_rows_3d_opened_at_year = Window.partitionBy("prior_rows_3d", F.year("opened_at")).orderBy("prior_rows_3d", "id")

joined_ranges = (
    df_tickets.alias("a")
    .join(
        df_tickets.alias("b"),
        (F.col("b.agent_id") == F.col("a.agent_id"))
        & (F.col("b.opened_at") < F.col("a.opened_at"))
        & (F.col("b.opened_at") >= F.col("a.opened_at") - F.expr("INTERVAL 3 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.agent_id", "a.opened_at")
    .agg(
        F.count("b.id").alias("prior_rows_3d"),
        F.sum("b.requester_id").alias("prior_requester_id_3d"),
    )
)

with_windows = (
    joined_ranges
    .select(
        "*",
        F.first("opened_at").over(w_all_by_opened_at_full).alias("first_opened_at"),
        F.rank().over(w_all_by_opened_at).alias("opened_at_rank"),
        F.ntile(10).over(w_prior_rows_3d_opened_at_year).alias("prior_rows_3d_decile"),
        F.sum("prior_requester_id_3d").over(w_opened_at_to_date_agent_id).alias("opened_at_to_date_agent_id_sum_prior_requester_i"),
        F.round(F.percent_rank().over(w_prior_rows_3d_opened_at_year), 3).alias("prior_rows_3d_percent_rank"),
    )
)

with_activity = (
    with_windows
    .crossJoin(with_windows.agg(F.avg("prior_requester_id_3d").alias("overall_avg_prior_requester_id_3d")))
    .filter(F.col("prior_requester_id_3d") > F.col("overall_avg_prior_requester_id_3d"))
    .drop("overall_avg_prior_requester_id_3d")
)

with_activity_2 = (
    with_activity.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.actor") == "customer") & (F.hour(F.col("t.event_at")).between(10, 14)), "left_semi")
)

result = (
    with_activity_2
    .select(
        "id",
        F.col("opened_at_rank").alias("opened_at_rank_v"),
        F.col("prior_requester_id_3d").alias("m_prior_requester_id_3d"),
    )
    .unpivot(
        ids=["id"],
        values=["opened_at_rank_v", "m_prior_requester_id_3d"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
