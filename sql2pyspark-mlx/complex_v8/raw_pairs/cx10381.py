from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_closed_at_date_year = Window.partitionBy("channel", F.year("closed_at_date"))
w_all_by_opened_at_running = Window.orderBy(F.col("opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_opened_at_month_by_opened_at = Window.partitionBy(F.month("opened_at")).orderBy("opened_at", "id").rowsBetween(-1, Window.currentRow)

filtered_tickets = (
    df_tickets
    .filter(F.col("csat") >= 2)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "channel",
        F.round(F.col("csat") / F.nullif(F.col("requester_id"), F.lit(0)), 3).alias("csat_per_requester_id"),
        F.dayofweek(F.col("closed_at")).alias("closed_at_dayofweek"),
        F.to_date(F.col("closed_at")).alias("closed_at_date"),
    )
)

with_activity = (
    filtered_tickets.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.actor") == "agent") & (F.col("t.event_type").isin("resolved", "replied", "escalated")), "left_semi")
)

with_trends = (
    with_activity
    .select(
        "id",
        "requester_id",
        "agent_id",
        "channel",
        "csat_per_requester_id",
        "closed_at_dayofweek",
        F.size(F.collect_set("agent_id").over(w_channel_closed_at_date_year)).alias("distinct_agent_id_in_channel_closed_at_date_year"),
        F.first("closed_at_dayofweek", ignorenulls=True).over(w_all_by_opened_at_running).alias("first_known_closed_at_dayofweek"),
        F.count(F.lit(1)).over(w_channel_closed_at_date_year).alias("rows_in_channel_closed_at_date_year"),
        F.round(F.col("csat_per_requester_id") / F.nullif(F.sum("csat_per_requester_id").over(w_channel_closed_at_date_year), F.lit(0)), 4).alias("csat_per_requester_id_share_of_channel_closed_at"),
        F.max("csat_per_requester_id").over(w_opened_at_month_by_opened_at).alias("csat_per_requester_id_max_2"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "requester_id",
        "first_known_closed_at_dayofweek",
        "distinct_agent_id_in_channel_closed_at_date_year",
        "rows_in_channel_closed_at_date_year",
    )
    .unpivot(
        ids=["id"],
        values=["requester_id", "first_known_closed_at_dayofweek", "distinct_agent_id_in_channel_closed_at_date_year", "rows_in_channel_closed_at_date_year"],
        variableColumnName="field",
        valueColumnName="value",
    )
)
