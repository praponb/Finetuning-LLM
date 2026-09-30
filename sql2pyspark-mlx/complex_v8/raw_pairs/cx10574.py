from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_channel = Window.partitionBy("agent_id", "channel")
w_channel_opened_at_year_by_cs = Window.partitionBy("channel", F.year("opened_at")).orderBy("csat", "id").rowsBetween(-3, -1)

valid = (
    df_tickets
    .filter(F.col("priority") != "P4")
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
    )
)

without_matches = (
    valid
    .crossJoin(valid.agg(F.min("csat").alias("overall_min_csat")))
    .filter(F.col("csat") > F.col("overall_min_csat"))
    .drop("overall_min_csat")
)

with_windows = (
    without_matches
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.round(F.col("csat") - F.avg("csat").over(w_agent_id_channel), 2).alias("csat_vs_agent_id_channel_avg"),
        F.sum("csat").over(w_agent_id_channel).alias("agent_id_channel_sum_csat"),
        F.sum("csat").over(w_channel_opened_at_year_by_cs).alias("csat_sum_3_prior"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count("*").alias("ticket_count"),
        F.count("*").alias("ticket_count_2"),
    )
)

summarized_rows = (
    with_windows.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
        F.coalesce(F.col("t.ticket_count_2"), F.lit(0)).alias("ticket_count_2"),
    )
)

enriched = (
    summarized_rows
    .filter(F.col("ticket_count") < 7)
    .select(
        "*",
        F.concat(F.col("priority"), F.lit("#"), F.col("csat").cast("string")).alias("priority_ref"),
        F.date_trunc("week", F.col("opened_at")).alias("opened_at_week"),
    )
)

result = (
    enriched
    .select(
        "id",
        "priority",
        "channel",
        "csat_vs_agent_id_channel_avg",
        "csat_sum_3_prior",
        "ticket_count",
        "ticket_count_2",
        F.length(F.trim(F.col("priority"))).alias("priority_len"),
    )
    .orderBy(F.col("csat_sum_3_prior").asc_nulls_last(), F.col("priority").desc())
)
