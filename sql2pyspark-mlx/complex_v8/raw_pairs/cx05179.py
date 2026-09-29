from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_opened_at_year_by_c = Window.partitionBy("agent_id", F.year("opened_at")).orderBy("closed_at", "id")
w_agent_id_csat = Window.partitionBy("agent_id", "csat")
w_agent_id_opened_at_year_by_c_2 = Window.partitionBy("agent_id", F.year("opened_at")).orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_opened_at_year_by_c_3 = Window.partitionBy("agent_id", F.year("opened_at")).orderBy("closed_at")

cleaned_tickets = (
    df_tickets
    .filter(F.col("requester_id").between(4, 23))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "csat",
        F.date_format(F.col("closed_at"), "yyyy-MM-dd").alias("closed_at_label"),
        F.concat(F.col("channel"), F.lit("#"), F.col("csat").cast("string")).alias("channel_ref"),
        F.size(F.col("tags")).alias("tags_count"),
    )
)

metrics = (
    cleaned_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "csat",
        "closed_at_label",
        "channel_ref",
        "tags_count",
        (F.col("requester_id") - F.lag("requester_id").over(w_agent_id_opened_at_year_by_c)).alias("requester_id_change"),
        F.count(F.lit(1)).over(w_agent_id_csat).alias("rows_in_agent_id_csat"),
        F.nth_value("closed_at", 2).over(w_agent_id_opened_at_year_by_c_2).alias("second_closed_at"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_agent_id_csat), 2).alias("requester_id_vs_agent_id_csat_avg"),
        F.round(F.percent_rank().over(w_agent_id_opened_at_year_by_c_3), 3).alias("closed_at_percent_rank"),
    )
)

with_activity = (
    metrics.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.event_type").isin("replied", "resolved", "assigned")), "left_anti")
)

result = (
    with_activity
    .select("id", "closed_at_percent_rank", "requester_id_vs_agent_id_csat_avg")
    .unpivot(
        ids=["id"],
        values=["closed_at_percent_rank", "requester_id_vs_agent_id_csat_avg"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
)
