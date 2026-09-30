from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_agent_id_by_closed = Window.partitionBy("priority", "agent_id").orderBy(F.col("closed_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_priority_agent_id_by_closed_2 = Window.partitionBy("priority", "agent_id").orderBy(F.col("closed_at").asc_nulls_last(), "id")

prepared = (
    df_tickets
    .filter(F.col("requester_id") < 15)
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "tags",
        F.concat_ws(": ", F.col("channel"), F.col("priority")).alias("channel_priority_label"),
    )
)

with_trends = (
    prepared
    .select(
        "id",
        "agent_id",
        "closed_at",
        "tags",
        F.nth_value("closed_at", 2).over(w_priority_agent_id_by_closed).alias("second_closed_at"),
        F.lag("closed_at").over(w_priority_agent_id_by_closed_2).alias("prev_closed_at"),
        F.lead("closed_at").over(w_priority_agent_id_by_closed_2).alias("next_closed_at"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(F.max("closed_at").alias("last_ticket_closed_at"))
)

summarized_rows = (
    with_trends.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select("s.*", "t.last_ticket_closed_at")
)

labeled = (
    summarized_rows
    .filter(F.month(F.col("prev_closed_at")).isin(2, 3))
    .select(
        "id",
        "agent_id",
        "closed_at",
        "tags",
        "second_closed_at",
        "prev_closed_at",
        "next_closed_at",
        "last_ticket_closed_at",
        F.date_format(F.col("next_closed_at"), "yyyy-MM-dd HH:00").alias("next_closed_at_label"),
        F.size(F.col("tags")).alias("tags_count"),
    )
)

result = (
    labeled
    .select(
        "id",
        "agent_id",
        "closed_at",
        "second_closed_at",
        "next_closed_at",
        "last_ticket_closed_at",
        "next_closed_at_label",
        F.substring(F.trim(F.col("next_closed_at_label")), 1, 3).alias("next_closed_at_label_prefix"),
    )
    .orderBy(F.col("agent_id").desc(), "last_ticket_closed_at")
)
