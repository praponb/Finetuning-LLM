from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority = Window.partitionBy("priority")
w_agent_id_by_csat_minus_reque = Window.partitionBy("agent_id").orderBy("csat_minus_requester_id", "id").rowsBetween(-1, -1)

prepared_tickets = (
    df_tickets
    .filter(F.col("csat").between(1, 4))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        "tags",
        (F.col("csat") - F.col("requester_id")).alias("csat_minus_requester_id"),
    )
)

metrics = (
    prepared_tickets
    .select(
        "*",
        F.max("csat_minus_requester_id").over(w_priority).alias("priority_max_csat_minus_requester_id"),
        F.round(F.avg("csat_minus_requester_id").over(w_agent_id_by_csat_minus_reque), 2).alias("csat_minus_requester_id_avg_1_prior"),
        F.round(F.avg("csat").over(w_priority), 2).alias("priority_avg_csat"),
    )
)

summary = (
    metrics
    .groupBy("agent_id", "csat")
    .agg(
        F.count(F.when(F.col("priority").isNotNull(), 1)).alias("n_priority_is_not_null"),
        F.round(F.avg("csat_minus_requester_id"), 2).alias("avg_csat_minus_requester_id"),
        F.sum("priority_max_csat_minus_requester_id").alias("sum_priority_max_csat_minus_requester_id_check"),
    )
    .filter(F.col("sum_priority_max_csat_minus_requester_id_check") > 31)
    .drop("sum_priority_max_csat_minus_requester_id_check")
)

detail_rows_tickets = (
    summary.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.agent_id",
        "s.csat",
        "s.n_priority_is_not_null",
        "s.avg_csat_minus_requester_id",
        F.col("t.id").alias("ticket_id"),
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.channel").alias("ticket_channel"),
    )
)

result = (
    detail_rows_tickets
    .select(
        "agent_id",
        "n_priority_is_not_null",
        "avg_csat_minus_requester_id",
        "ticket_id",
        "ticket_tags",
        "ticket_channel",
    )
    .orderBy("ticket_channel")
)
