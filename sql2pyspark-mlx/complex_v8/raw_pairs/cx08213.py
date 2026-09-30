from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_channel = Window.partitionBy("agent_id", "channel")
w_ticket_count_by_ticket_count = Window.partitionBy("ticket_count").orderBy(F.col("ticket_count").asc_nulls_last(), "agent_id", "channel", "opened_at_year")
w_ticket_count_by_ticket_count_2 = Window.partitionBy("ticket_count").orderBy(F.col("ticket_count").asc_nulls_last(), "agent_id", "channel", "opened_at_year").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent = (
    df_tickets
    .filter((F.array_contains(F.col("tags"), "upgrade")) | (F.col("requester_id") >= 3))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        F.substring(F.trim(F.col("channel")), 1, 3).alias("channel_prefix"),
    )
)

grouped = (
    recent
    .filter(F.month(F.col("opened_at")).isin(1, 2))
    .groupBy("agent_id", "channel", F.year("opened_at").alias("opened_at_year"))
    .agg(
        F.max("opened_at").alias("last_opened_at"),
        F.array_sort(F.collect_set("priority")).alias("priority_list"),
        F.sum(F.when(F.col("channel") == "email", F.col("requester_id")).otherwise(0)).alias("requester_id_channel_email"),
        F.sum("requester_id").alias("sum_requester_id_check"),
    )
    .filter(F.col("sum_requester_id_check") > 8)
    .drop("sum_requester_id_check")
)

without_matches = (
    grouped.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.csat") < 5) & (F.col("t.opened_at") < F.lit("2025-02-12 14:00:00").cast("timestamp")), "left_semi")
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.round(F.avg("csat"), 2).alias("avg_ticket_csat"),
        F.max("opened_at").alias("last_ticket_opened_at"),
        F.count("*").alias("ticket_count"),
    )
)

with_stats = (
    without_matches.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        "t.avg_ticket_csat",
        "t.last_ticket_opened_at",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
    )
)

with_trends = (
    with_stats
    .select(
        "agent_id",
        "channel",
        "opened_at_year",
        "priority_list",
        "requester_id_channel_email",
        "avg_ticket_csat",
        "last_ticket_opened_at",
        "ticket_count",
        F.ntile(5).over(w_ticket_count_by_ticket_count).alias("ticket_count_quintile"),
        F.nth_value("opened_at_year", 2).over(w_ticket_count_by_ticket_count_2).alias("second_opened_at_year"),
    )
)

result = (
    with_trends
    .groupBy("agent_id")
    .pivot("channel", ["phone", "web"])
    .agg(F.max("requester_id_channel_email"))
    .orderBy("agent_id")
)
