from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel = Window.partitionBy("channel")
w_opened_at_year_by_opened_at = Window.partitionBy(F.year("opened_at")).orderBy(F.col("opened_at").desc(), "id")
w_priority_len_priority_by_clo = Window.partitionBy("priority_len", "priority").orderBy("closed_at")
w_agent_id_channel_by_opened_a = Window.partitionBy("agent_id", "channel").orderBy(F.col("opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_closed_at_year_priority_len = Window.partitionBy(F.year("closed_at"), "priority_len").orderBy("closed_at", "id")
w_agent_id_channel_by_opened_a_2 = Window.partitionBy("agent_id", "channel").orderBy(F.col("opened_at").desc(), "id").rowsBetween(-2, -1)
w_priority_len_last_opened_at = Window.partitionBy("priority_len", F.to_date("last_opened_at"))
w_all_by_row_count = Window.orderBy(F.col("row_count").asc_nulls_last(), "agent_id", "priority_len", "prev_priority_len")
w_all_by_row_count_running = Window.orderBy(F.col("row_count").asc_nulls_last(), "agent_id", "priority_len", "prev_priority_len").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_tickets = (
    df_tickets
    .filter(F.size(F.col("tags")) >= 1)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        F.length(F.trim(F.col("priority"))).alias("priority_len"),
    )
)

sequenced = (
    scoped_tickets
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority_len",
        F.max("requester_id").over(w_channel).alias("channel_max_requester_id"),
        F.round(F.percent_rank().over(w_opened_at_year_by_opened_at), 3).alias("opened_at_percent_rank"),
        F.round(F.percent_rank().over(w_priority_len_priority_by_clo), 3).alias("closed_at_percent_rank"),
        F.first("opened_at", ignorenulls=True).over(w_agent_id_channel_by_opened_a).alias("first_known_opened_at"),
        F.round(F.col("priority_len") - F.avg("priority_len").over(w_channel), 2).alias("priority_len_vs_channel_avg"),
        F.lag("priority_len").over(w_closed_at_year_priority_len).alias("prev_priority_len"),
        F.round(F.avg("priority_len").over(w_agent_id_channel_by_opened_a_2), 2).alias("priority_len_avg_2_prior"),
        F.round(F.avg("requester_id").over(w_channel), 2).alias("channel_avg_requester_id"),
    )
)

enriched = (
    sequenced
    .select(
        "*",
        F.date_format(F.col("opened_at"), "yyyy-MM").alias("opened_at_label"),
        (F.floor(F.col("channel_avg_requester_id") / 5) * 5).alias("channel_avg_requester_id_bucket"),
        F.date_format(F.col("first_known_opened_at"), "yyyy-MM-dd HH:00").alias("first_known_opened_at_label"),
    )
)

rollup_base = (
    enriched
    .groupBy("agent_id", "priority_len", "prev_priority_len")
    .agg(
        F.count("*").alias("row_count"),
        F.min("channel_max_requester_id").alias("min_channel_max_requester_id"),
        F.countDistinct("agent_id").alias("distinct_agent_ids"),
        F.max("opened_at").alias("last_opened_at"),
        F.min("opened_at").alias("first_opened_at"),
    )
)

with_windows = (
    rollup_base
    .select(
        "*",
        F.count(F.lit(1)).over(w_priority_len_last_opened_at).alias("rows_in_priority_len_last_opened_at_to_date"),
        (F.col("prev_priority_len") - F.lag("prev_priority_len").over(w_all_by_row_count)).alias("prev_priority_len_change"),
        F.first("row_count", ignorenulls=True).over(w_all_by_row_count_running).alias("first_known_row_count"),
    )
)

result = (
    with_windows
    .filter(F.col("agent_id") <= 6)
    .select(
        "agent_id",
        "priority_len",
        "row_count",
        "min_channel_max_requester_id",
        "distinct_agent_ids",
        "last_opened_at",
        "prev_priority_len_change",
        "first_known_row_count",
        F.coalesce(F.col("first_known_row_count"), F.lit(0)).alias("first_known_row_count_filled"),
        F.date_format(F.col("first_opened_at"), "MMM yyyy").alias("first_opened_at_label"),
    )
    .orderBy(
        F.col("priority_len").desc_nulls_first(),
        F.col("first_known_row_count_filled").asc(),
    )
)
