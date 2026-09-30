from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tier_or_unknown_last_ticket = Window.partitionBy("tier_or_unknown", F.month("last_ticket_closed_at")).orderBy(F.col("hired_on_plus_30d_month_end").asc_nulls_last(), "id")
w_tier_or_unknown_last_ticket_2 = Window.partitionBy("tier_or_unknown", F.month("last_ticket_closed_at")).orderBy(F.col("hired_on_plus_30d_month_end").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_tier_or_unknown_last_ticket_3 = Window.partitionBy("tier_or_unknown", F.month("last_ticket_closed_at")).orderBy(F.col("hired_on_plus_30d_month_end").asc_nulls_last())
w_tier_or_unknown_last_ticket_4 = Window.partitionBy("tier_or_unknown", F.month("last_ticket_closed_at")).orderBy(F.col("hired_on_plus_30d_month_end").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_agents = (
    df_support_agents
    .filter((F.lower(F.col("agent_name")).like("%at%")) & (F.col("team") == "Billing"))
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        F.date_add(F.col("hired_on"), 30).alias("hired_on_plus_30d"),
    )
)

tagged = (
    filtered_agents
    .select(
        "id",
        "tier",
        "hired_on_plus_30d",
        F.concat_ws("|", F.col("agent_name"), F.col("tier")).alias("agent_name_tier_label"),
    )
)

labeled = (
    tagged
    .select(
        "id",
        "tier",
        "agent_name_tier_label",
        F.coalesce(F.col("tier"), F.lit("UNKNOWN")).alias("tier_or_unknown"),
        F.last_day(F.col("hired_on_plus_30d")).alias("hired_on_plus_30d_month_end"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count(F.when(F.col("channel").isin("email", "chat"), 1)).alias("ticket_channel_count"),
    )
)

with_stats = (
    labeled.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_channel_count"), F.lit(0)).alias("ticket_channel_count"),
    )
)

ticket_stats_2 = (
    df_tickets
    .groupBy("agent_id")
    .agg(F.max("closed_at").alias("last_ticket_closed_at"))
)

with_stats_2 = (
    with_stats.alias("s")
    .join(ticket_stats_2.alias("t"), F.col("t.agent_id") == F.col("s.id"), "left")
    .select("s.*", "t.last_ticket_closed_at")
)

with_windows = (
    with_stats_2
    .select(
        "id",
        "tier",
        "agent_name_tier_label",
        "tier_or_unknown",
        "hired_on_plus_30d_month_end",
        "ticket_channel_count",
        "last_ticket_closed_at",
        F.row_number().over(w_tier_or_unknown_last_ticket).alias("seq_no"),
        F.lag("ticket_channel_count").over(w_tier_or_unknown_last_ticket).alias("prev_ticket_channel_count"),
        F.last("last_ticket_closed_at", ignorenulls=True).over(w_tier_or_unknown_last_ticket_2).alias("last_known_last_ticket_closed_at"),
        F.dense_rank().over(w_tier_or_unknown_last_ticket_3).alias("hired_on_plus_30d_month_end_dense_rank"),
        F.nth_value("last_ticket_closed_at", 2).over(w_tier_or_unknown_last_ticket_4).alias("second_last_ticket_closed_at"),
    )
)

result = (
    with_windows
    .groupingSets(
        [["tier_or_unknown", "ticket_channel_count", "hired_on_plus_30d_month_end_dense_rank"], ["tier_or_unknown"], ["ticket_channel_count"]],
        "tier_or_unknown", "ticket_channel_count", "hired_on_plus_30d_month_end_dense_rank",
    )
    .agg(
        F.sum(F.when(F.col("tier") == "T1", F.col("hired_on_plus_30d_month_end_dense_rank")).otherwise(0)).alias("hired_on_plus_30d_month_end_dense_rank_tier_t1"),
    )
    .select(
        F.coalesce(F.col("tier_or_unknown"), F.lit("ALL")).alias("tier_or_unknown"),
        "ticket_channel_count",
        "hired_on_plus_30d_month_end_dense_rank",
        "hired_on_plus_30d_month_end_dense_rank_tier_t1",
    )
    .orderBy(
        F.col("tier_or_unknown").asc_nulls_last(),
        F.col("ticket_channel_count").asc_nulls_last(),
        F.col("hired_on_plus_30d_month_end_dense_rank").asc_nulls_last(),
    )
)
