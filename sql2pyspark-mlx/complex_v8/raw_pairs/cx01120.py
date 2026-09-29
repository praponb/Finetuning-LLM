from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_opened_at_full = Window.orderBy(F.col("opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_channel = Window.partitionBy("channel")
w_all_by_opened_at = Window.orderBy(F.col("opened_at").desc(), "id")
w_all_by_opened_at_running = Window.orderBy(F.col("opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_priority_csat_by_csat = Window.partitionBy("priority", "csat").orderBy(F.col("csat").desc(), "id")
w_channel_by_next2_csat = Window.partitionBy("channel").orderBy(F.col("next2_csat").desc(), "id")
w_opened_at_seq_next2_csat_by = Window.partitionBy("opened_at_seq", "next2_csat").orderBy(F.col("second_csat").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_next2_csat_agent_id = Window.partitionBy("next2_csat", "agent_id")
w_next2_csat_by_csat_prior_3 = Window.partitionBy("next2_csat").orderBy(F.col("csat").desc(), "id").rowsBetween(-3, -1)

filtered = (
    df_tickets
    .filter(F.col("opened_at") < F.lit("2025-02-24 15:00:00").cast("timestamp"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.concat(F.col("channel"), F.lit("#"), F.col("agent_id").cast("string")).alias("channel_ref"),
    )
)

with_windows = (
    filtered
    .select(
        "id",
        "agent_id",
        "opened_at",
        "channel",
        "csat",
        "tags",
        "channel_ref",
        F.nth_value("csat", 2).over(w_all_by_opened_at_full).alias("second_csat"),
        F.min("csat").over(w_channel).alias("channel_min_csat"),
        F.max("csat").over(w_channel).alias("channel_max_csat"),
        F.row_number().over(w_all_by_opened_at).alias("opened_at_seq"),
        F.sum("csat").over(w_all_by_opened_at_running).alias("running_csat"),
        F.lead("csat", 2).over(w_priority_csat_by_csat).alias("next2_csat"),
    )
)

sequenced = (
    with_windows
    .select(
        "id",
        "agent_id",
        "opened_at",
        "csat",
        "tags",
        "channel_ref",
        "channel_min_csat",
        "opened_at_seq",
        "running_csat",
        F.ntile(5).over(w_channel_by_next2_csat).alias("next2_csat_quintile"),
        F.last("channel_min_csat", ignorenulls=True).over(w_opened_at_seq_next2_csat_by).alias("last_known_channel_min_csat"),
        F.rank().over(w_channel_by_next2_csat).alias("next2_csat_rank"),
        F.count(F.lit(1)).over(w_next2_csat_agent_id).alias("rows_in_next2_csat_agent_id"),
        F.sum("channel_max_csat").over(w_next2_csat_by_csat_prior_3).alias("channel_max_csat_sum_3_prior"),
    )
)

rollup_base = (
    sequenced
    .filter(F.col("channel_min_csat") < 28)
    .groupBy(F.to_date("opened_at").alias("opened_at_day"))
    .agg(
        F.countDistinct("agent_id").alias("distinct_agent_ids"),
        F.max("opened_at").alias("last_opened_at"),
        F.sum("last_known_channel_min_csat").alias("total_last_known_channel_min_csat"),
        F.min("opened_at").alias("first_opened_at"),
        F.avg("last_known_channel_min_csat").alias("avg_last_known_channel_min_csat_check"),
    )
    .filter(F.col("avg_last_known_channel_min_csat_check") > 16)
    .drop("avg_last_known_channel_min_csat_check")
)

result = (
    rollup_base
    .filter(F.col("total_last_known_channel_min_csat") <= 180)
    .select(
        "opened_at_day",
        "last_opened_at",
        "total_last_known_channel_min_csat",
        "first_opened_at",
        F.trunc(F.col("opened_at_day"), "MM").alias("opened_at_day_month_start"),
    )
)
