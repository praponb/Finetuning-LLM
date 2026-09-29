from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_opened_at_month = Window.partitionBy(F.month("opened_at"))
w_prior_rows_3d_agent_id_by_pr = Window.partitionBy("prior_rows_3d", "agent_id").orderBy("prior_rows_3d", "id")

matched = (
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

with_trends = (
    matched
    .select(
        "*",
        F.count(F.lit(1)).over(w_opened_at_month).alias("rows_in_opened_at_month"),
        F.size(F.collect_set("agent_id").over(w_opened_at_month)).alias("distinct_agent_id_in_opened_at_month"),
        F.max("prior_requester_id_3d").over(w_opened_at_month).alias("opened_at_month_max_prior_requester_id_3d"),
        F.lead("opened_at", 3).over(w_prior_rows_3d_agent_id_by_pr).alias("next3_opened_at"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.count(F.when(F.col("actor").isin("agent", "customer"), 1)).alias("event_actor_count"),
        F.count("*").alias("event_count"),
    )
)

with_stats = (
    with_trends.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "t.first_event_at",
        F.coalesce(F.col("t.event_actor_count"), F.lit(0)).alias("event_actor_count"),
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

result = (
    with_stats
    .select(
        "id",
        "agent_id",
        "opened_at",
        "prior_rows_3d",
        "prior_requester_id_3d",
        "distinct_agent_id_in_opened_at_month",
        "opened_at_month_max_prior_requester_id_3d",
        "first_event_at",
        "event_actor_count",
        "event_count",
        F.date_format(F.col("first_event_at"), "MMM yyyy").alias("first_event_at_label"),
        F.coalesce(F.col("prior_rows_3d"), F.lit(0)).alias("prior_rows_3d_filled"),
    )
    .orderBy(F.col("first_event_at").desc())
)
