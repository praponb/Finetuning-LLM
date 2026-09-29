from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_count_by_event_count_2 = Window.partitionBy("event_count").orderBy(F.col("event_count_2").desc())
w_agent_id = Window.partitionBy("agent_id")
w_priority_by_event_count_3 = Window.partitionBy("priority").orderBy("event_count_3", "id")
w_event_count_by_event_count_2_2 = Window.partitionBy("event_count").orderBy(F.col("event_count_2").desc(), "id")

base = (
    df_tickets
    .filter((F.col("channel") != "web") & (F.col("priority") != "P3"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "tags",
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count("*").alias("event_count"),
        F.count("*").alias("event_count_2"),
        F.count("*").alias("event_count_3"),
    )
)

with_stats = (
    base.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        F.coalesce(F.col("t.event_count_2"), F.lit(0)).alias("event_count_2"),
        F.coalesce(F.col("t.event_count_3"), F.lit(0)).alias("event_count_3"),
    )
)

with_trends = (
    with_stats
    .select(
        "*",
        F.dense_rank().over(w_event_count_by_event_count_2).alias("event_count_2_dense_rank"),
        F.sum("event_count_3").over(w_agent_id).alias("agent_id_sum_event_count_3"),
        F.ntile(10).over(w_priority_by_event_count_3).alias("event_count_3_decile"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_agent_id), 2).alias("requester_id_vs_agent_id_avg"),
        F.round(F.col("event_count") - F.avg("event_count").over(w_agent_id), 2).alias("event_count_vs_agent_id_avg"),
        F.lead("event_count").over(w_event_count_by_event_count_2_2).alias("next_event_count"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "requester_id",
        "tags",
        "event_count_2",
        "event_count_2_dense_rank",
        "requester_id_vs_agent_id_avg",
        "next_event_count",
        F.substring(F.trim(F.col("priority")), 1, 3).alias("priority_prefix"),
        F.greatest(F.col("event_count_2"), F.col("event_count")).alias("greatest_event_count_2_event_count"),
    )
    .orderBy(
        F.col("greatest_event_count_2_event_count").desc(),
        F.col("priority_prefix").asc_nulls_last(),
    )
)
