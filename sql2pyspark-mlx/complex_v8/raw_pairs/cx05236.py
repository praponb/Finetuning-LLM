from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("event_type").orderBy("ticket_closed_at", "id")
w_event_type_by_ticket_closed = Window.partitionBy("event_type").orderBy("ticket_closed_at")
w_all_by_agent_hired_on_ties = Window.orderBy(F.col("agent_hired_on").desc())
w_event_type_by_ticket_closed_2 = Window.partitionBy("event_type").orderBy("ticket_closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base_events = df_ticket_events.filter(F.col("event_type").isin("assigned", "reopened", "replied"))

joined = (
    base_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.channel").alias("ticket_channel"),
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.team").alias("agent_team"),
    )
)

deduped = (
    joined
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

metrics = (
    deduped
    .select(
        "*",
        F.rank().over(w_event_type_by_ticket_closed).alias("ticket_closed_at_rank"),
        F.round(F.cume_dist().over(w_all_by_agent_hired_on_ties), 3).alias("agent_hired_on_cume_dist"),
        F.last("agent_hired_on").over(w_event_type_by_ticket_closed_2).alias("last_agent_hired_on"),
        F.rank().over(w_latest).alias("ticket_closed_at_rank_2"),
    )
)

result = (
    metrics
    .select("id", "ticket_closed_at_rank", "ticket_closed_at_rank_2")
    .unpivot(
        ids=["id"],
        values=["ticket_closed_at_rank", "ticket_closed_at_rank_2"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "measure")
)
