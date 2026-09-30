from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_csat_by_avg_ticket_csat = Window.partitionBy("csat").orderBy(F.col("avg_ticket_csat").desc(), "id")
w_agent_id_csat_by_opened_at = Window.partitionBy("agent_id", "csat").orderBy("opened_at", "id")
w_min_ticket_csat_ticket_prior = Window.partitionBy("min_ticket_csat", "ticket_priority_count")
w_last_ticket_closed_at_month = Window.partitionBy(F.month("last_ticket_closed_at"), "n_channel_is_not_null").orderBy("first_opened_at", "agent_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_priority_count_by_fir = Window.partitionBy("ticket_priority_count").orderBy(F.col("first_opened_at").desc_nulls_first(), "agent_id")

recent = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "csat",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("closed_at")).alias("days_since_closed_at"),
        F.round((F.unix_timestamp(F.col("opened_at")) - F.unix_timestamp(F.col("closed_at"))) / 3600, 1).alias("hours_closed_at_to_opened_at"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(F.round(F.avg("csat"), 2).alias("avg_ticket_csat"))
)

with_counts = (
    recent.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select("s.*", "t.avg_ticket_csat")
)

enriched = (
    with_counts
    .select(
        "*",
        F.concat(F.col("channel"), F.lit("#"), F.col("id").cast("string")).alias("channel_ref"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "csat",
        "days_since_closed_at",
        "hours_closed_at_to_opened_at",
        "avg_ticket_csat",
        F.dense_rank().over(w_csat_by_avg_ticket_csat).alias("avg_ticket_csat_dense_rank"),
        (F.col("requester_id") - F.lag("requester_id").over(w_agent_id_csat_by_opened_at)).alias("requester_id_change"),
    )
)

summary = (
    with_windows
    .groupBy("agent_id")
    .agg(
        F.sum(F.when(F.col("channel").isNotNull(), 1).otherwise(0)).alias("n_channel_is_not_null"),
        F.min("opened_at").alias("first_opened_at"),
        F.sum(F.when(F.col("channel") == "chat", F.col("requester_id")).otherwise(0)).alias("requester_id_channel_chat"),
        F.round(F.stddev("days_since_closed_at"), 2).alias("stddev_days_since_closed_at"),
        F.max("hours_closed_at_to_opened_at").alias("max_hours_closed_at_to_opened_at_check"),
    )
    .filter(F.col("max_hours_closed_at_to_opened_at_check") > -1200.0)
    .drop("max_hours_closed_at_to_opened_at_check")
)

ticket_stats_2 = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count(F.when(F.col("priority").isin("P2", "P4"), 1)).alias("ticket_priority_count"),
        F.min("csat").alias("min_ticket_csat"),
        F.max("closed_at").alias("last_ticket_closed_at"),
    )
)

with_stats = (
    summary.alias("s")
    .join(ticket_stats_2.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_priority_count"), F.lit(0)).alias("ticket_priority_count"),
        "t.min_ticket_csat",
        "t.last_ticket_closed_at",
    )
)

with_trends = (
    with_stats
    .select(
        "*",
        F.round(F.col("min_ticket_csat") - F.avg("min_ticket_csat").over(w_min_ticket_csat_ticket_prior), 2).alias("min_ticket_csat_vs_min_ticket_csat_ticket_priori"),
        F.sum("n_channel_is_not_null").over(w_last_ticket_closed_at_month).alias("running_n_channel_is_not_null"),
        F.lead("stddev_days_since_closed_at", 3).over(w_ticket_priority_count_by_fir).alias("next3_stddev_days_since_closed_at"),
    )
)

result = (
    with_trends
    .filter(F.col("n_channel_is_not_null") <= 6)
    .select(
        "agent_id",
        "n_channel_is_not_null",
        "requester_id_channel_chat",
        "stddev_days_since_closed_at",
        "ticket_priority_count",
        "min_ticket_csat",
        "last_ticket_closed_at",
        "min_ticket_csat_vs_min_ticket_csat_ticket_priori",
        "running_n_channel_is_not_null",
        (F.col("ticket_priority_count") + F.col("n_channel_is_not_null")).alias("ticket_priority_count_plus_n_channel_is_not_null"),
    )
    .orderBy(
        "agent_id",
        F.col("ticket_priority_count_plus_n_channel_is_not_null").desc(),
    )
)
