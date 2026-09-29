from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("agent_id").orderBy(F.col("opened_at").desc(), F.col("id").desc())
w_dedup_2 = Window.partitionBy("agent_id").orderBy("total_ticket_requester_id", "id")
w_dedup_3 = Window.partitionBy("agent_id").orderBy(F.col("total_ticket_requester_id").desc(), F.col("id").desc())

cleaned = (
    df_tickets
    .filter((F.col("channel").isin("web", "email")) & (F.col("csat") <= 4))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "tags",
    )
)

latest_per_key = (
    cleaned
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

joined = (
    latest_per_key.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.hired_on") < F.lit("2025-03-30").cast("date")), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.tags",
        "su.agent_name",
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count("*").alias("ticket_count"),
        F.sum("requester_id").alias("total_ticket_requester_id"),
        F.count("*").alias("ticket_count_2"),
    )
)

with_stats = (
    joined.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
        "t.total_ticket_requester_id",
        F.coalesce(F.col("t.ticket_count_2"), F.lit(0)).alias("ticket_count_2"),
    )
)

latest = (
    with_stats
    .withColumn("recency_rank", F.row_number().over(w_dedup_2))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

latest_per_key_2 = (
    latest
    .withColumn("rn", F.row_number().over(w_dedup_3))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest_per_key_2
    .groupingSets(
        [["ticket_count", "ticket_count_2"], ["ticket_count"]],
        "ticket_count", "ticket_count_2",
    )
    .agg(
        F.min("ticket_count_2").alias("min_ticket_count_2"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
