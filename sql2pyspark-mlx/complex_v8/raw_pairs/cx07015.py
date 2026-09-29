from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_requester_id = Window.orderBy(F.col("requester_id").desc_nulls_first(), "id")
w_agent_id = Window.partitionBy("agent_id")

prepared_tickets = df_tickets.filter(F.col("requester_id") >= 7)

qualified = (
    prepared_tickets.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.event_type").isNotNull()), "left_anti")
)

windowed = (
    qualified
    .select(
        "*",
        (F.col("csat") - F.lag("csat").over(w_all_by_requester_id)).alias("csat_change"),
        F.size(F.collect_set("priority").over(w_agent_id)).alias("distinct_priority_in_agent_id"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_agent_id), 2).alias("requester_id_vs_agent_id_avg"),
        F.ntile(5).over(w_all_by_requester_id).alias("requester_id_quintile"),
        F.round(F.col("csat") - F.avg("csat").over(w_agent_id), 2).alias("csat_vs_agent_id_avg"),
    )
)

result = (
    windowed
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "csat",
        "tags",
        "csat_change",
        "distinct_priority_in_agent_id",
        "requester_id_vs_agent_id_avg",
        "requester_id_quintile",
        F.when(F.col("requester_id") >= 20, "large").when(F.col("requester_id") >= 9, "medium").otherwise("small").alias("requester_id_tier"),
    )
    .orderBy(F.col("requester_id").asc_nulls_last())
)
