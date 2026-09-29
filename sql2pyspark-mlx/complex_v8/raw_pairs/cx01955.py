from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("priority", "agent_id").orderBy(F.col("closed_at").desc(), F.col("id").desc())
w_ticket_count_agent_id = Window.partitionBy("ticket_count", "agent_id")
w_priority_ticket_priority_cou = Window.partitionBy("priority", "ticket_priority_count").orderBy("greatest_csat_requester_id", "id")

valid = (
    df_tickets
    .filter((F.col("agent_id") <= 6) & (~F.col("priority").isin("P1", "P2")))
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.greatest(F.col("csat"), F.col("requester_id")).alias("greatest_csat_requester_id"),
        F.greatest(F.col("csat"), F.col("requester_id")).alias("greatest_csat_requester_id_2"),
    )
)

latest = (
    valid
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(F.count("*").alias("ticket_count"))
)

with_stats = (
    latest.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
    )
)

qualified = (
    with_stats
    .crossJoin(with_stats.agg(F.avg("greatest_csat_requester_id_2").alias("overall_avg_greatest_csat_requester_id_2")))
    .filter(F.col("greatest_csat_requester_id_2") > F.col("overall_avg_greatest_csat_requester_id_2"))
    .drop("overall_avg_greatest_csat_requester_id_2")
)

ticket_stats_2 = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count(F.when(F.col("priority").isin("P3", "P4"), 1)).alias("ticket_priority_count"),
        F.count(F.when(F.col("priority").isNotNull(), 1)).alias("ticket_priority_count_2"),
    )
)

with_counts = (
    qualified.alias("s")
    .join(ticket_stats_2.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_priority_count"), F.lit(0)).alias("ticket_priority_count"),
        F.coalesce(F.col("t.ticket_priority_count_2"), F.lit(0)).alias("ticket_priority_count_2"),
    )
)

metrics = (
    with_counts
    .select(
        "*",
        F.count(F.lit(1)).over(w_ticket_count_agent_id).alias("rows_in_ticket_count_agent_id"),
        F.ntile(5).over(w_priority_ticket_priority_cou).alias("greatest_csat_requester_id_quintile"),
        F.round(F.avg("ticket_priority_count").over(w_ticket_count_agent_id), 2).alias("ticket_count_agent_id_avg_ticket_priority_count"),
        F.size(F.collect_set("channel").over(w_ticket_count_agent_id)).alias("distinct_channel_in_ticket_count_agent_id"),
    )
)

result = (
    metrics
    .select(
        "id",
        "csat",
        "ticket_priority_count",
        "rows_in_ticket_count_agent_id",
        "ticket_count",
    )
    .unpivot(
        ids=["id"],
        values=["csat", "ticket_priority_count", "rows_in_ticket_count_agent_id", "ticket_count"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "field")
)
