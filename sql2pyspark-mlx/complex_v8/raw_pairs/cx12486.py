from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_agent_id = Window.partitionBy("priority", "agent_id")
w_all_by_closed_at_full = Window.orderBy(F.col("closed_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_closed_at_to_date_channel_by = Window.partitionBy(F.to_date("closed_at"), "channel").orderBy("first_opened_at", "id")
w_closed_at_to_date_channel_by_2 = Window.partitionBy(F.to_date("closed_at"), "channel").orderBy("first_opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = (
    df_tickets
    .filter(F.col("channel").isin("web", "phone", "chat"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "tags",
    )
)

metrics = (
    scoped
    .select(
        "id",
        "opened_at",
        "closed_at",
        "channel",
        "tags",
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_priority_agent_id), 2).alias("requester_id_vs_priority_agent_id_avg"),
        F.first("opened_at").over(w_all_by_closed_at_full).alias("first_opened_at"),
    )
)

metrics_2 = (
    metrics
    .select(
        "*",
        F.ntile(4).over(w_closed_at_to_date_channel_by).alias("first_opened_at_quartile"),
        F.row_number().over(w_closed_at_to_date_channel_by).alias("first_opened_at_seq"),
        F.last("opened_at", ignorenulls=True).over(w_closed_at_to_date_channel_by_2).alias("last_known_opened_at"),
    )
)

enriched = (
    metrics_2
    .select(
        "*",
        F.coalesce(F.col("channel"), F.lit("UNKNOWN")).alias("channel_or_unknown"),
        F.abs(F.col("first_opened_at_quartile") - 6).alias("first_opened_at_quartile_distance"),
        F.hour(F.col("closed_at")).alias("closed_at_hour"),
    )
)

result = (
    enriched
    .filter(F.month(F.col("closed_at")).isin(1, 3))
    .select(
        "first_opened_at_quartile",
        "first_opened_at_seq",
        "first_opened_at_quartile_distance",
    )
    .distinct()
    .orderBy(
        "first_opened_at_quartile",
        F.col("first_opened_at_seq").desc_nulls_first(),
    )
)
