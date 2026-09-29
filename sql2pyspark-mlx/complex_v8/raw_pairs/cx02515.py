from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id")
w_all_by_csat = Window.orderBy("csat", "id")
w_agent_id_by_csat = Window.partitionBy("agent_id").orderBy("csat", "id")
w_agent_id_by_csat_full = Window.partitionBy("agent_id").orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared = (
    df_tickets
    .filter(~F.col("channel").isin("email", "web"))
    .select(
        "id",
        "agent_id",
        "channel",
        "csat",
        "tags",
        F.substring(F.trim(F.col("channel")), 1, 4).alias("channel_prefix"),
        F.to_date(F.col("closed_at")).alias("closed_at_date"),
    )
)

metrics = (
    prepared
    .select(
        "*",
        F.max("csat").over(w_agent_id).alias("agent_id_max_csat"),
        F.round(F.cume_dist().over(w_all_by_csat), 3).alias("csat_cume_dist"),
        F.datediff(F.col("closed_at_date"), F.lag("closed_at_date").over(w_all_by_csat)).alias("days_since_prev_closed_at_date"),
        F.datediff(F.col("closed_at_date"), F.lag("closed_at_date").over(w_agent_id_by_csat)).alias("days_since_prev_closed_at_date_2"),
        F.count(F.lit(1)).over(w_agent_id).alias("rows_in_agent_id"),
        F.nth_value("closed_at_date", 2).over(w_agent_id_by_csat_full).alias("second_closed_at_date"),
    )
)

with_activity = (
    metrics.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.actor").isin("agent", "system")) & (F.col("t.event_type") == "resolved"), "left_anti")
)

result = (
    with_activity
    .groupBy("closed_at_date")
    .pivot("channel", ["phone", "email"])
    .agg(F.sum("csat"))
    .orderBy("closed_at_date")
)
