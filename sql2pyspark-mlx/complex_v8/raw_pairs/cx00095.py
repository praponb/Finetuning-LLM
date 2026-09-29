from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_opened_at_year_by_o = Window.partitionBy("agent_id", F.year("opened_at")).orderBy("opened_at", "id")
w_agent_id_by_channel_len_full = Window.partitionBy("agent_id").orderBy(F.col("channel_len").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_opened_at_year_by_o_2 = Window.partitionBy("agent_id", F.year("opened_at")).orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_channel_len_agent_id = Window.partitionBy("channel_len", "agent_id")

cleaned_tickets = (
    df_tickets
    .filter((F.col("csat") >= 1) & (F.col("priority") == "P3"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "tags",
    )
)

derived = (
    cleaned_tickets
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "tags",
        F.length(F.trim(F.col("channel"))).alias("channel_len"),
        F.array_join(F.array_sort(F.col("tags")), ",").alias("tags_text"),
    )
)

sequenced = (
    derived
    .select(
        "id",
        "opened_at",
        "closed_at",
        "channel_len",
        (F.col("channel_len") - F.lag("channel_len").over(w_agent_id_opened_at_year_by_o)).alias("channel_len_change"),
        F.nth_value("channel_len", 2).over(w_agent_id_by_channel_len_full).alias("second_channel_len"),
        F.last("opened_at", ignorenulls=True).over(w_agent_id_opened_at_year_by_o_2).alias("last_known_opened_at"),
        F.count(F.lit(1)).over(w_channel_len_agent_id).alias("rows_in_channel_len_agent_id"),
        F.round(F.avg("channel_len").over(w_channel_len_agent_id), 2).alias("channel_len_agent_id_avg_channel_len"),
        F.round(F.col("channel_len") / F.nullif(F.sum("channel_len").over(w_channel_len_agent_id), F.lit(0)), 4).alias("channel_len_share_of_channel_len_agent_id"),
    )
)

tagged = (
    sequenced
    .filter(F.col("rows_in_channel_len_agent_id") >= 8)
    .select(
        "*",
        F.round(F.col("channel_len_change") / F.nullif(F.col("second_channel_len"), F.lit(0)), 3).alias("channel_len_change_per_second_channel_len"),
        F.coalesce(F.col("second_channel_len"), F.lit(0)).alias("second_channel_len_filled"),
    )
)

labeled = (
    tagged
    .filter(F.to_date(F.col("last_known_opened_at")) < F.lit("2025-03-08").cast("date"))
    .select(
        "*",
        (F.col("channel_len") + F.col("second_channel_len")).alias("channel_len_plus_second_channel_len"),
        F.when(F.col("channel_len_change") >= 7, "A").when(F.col("channel_len_change") >= -1, "B").otherwise("C").alias("channel_len_change_tier"),
    )
)

rollup_base = (
    labeled
    .groupBy(
        "second_channel_len",
        F.to_date("closed_at").alias("closed_at_day"),
        "second_channel_len_filled",
    )
    .agg(
        F.round(F.sum("channel_len_change_per_second_channel_len") / F.nullif(F.sum("second_channel_len"), F.lit(0)), 3).alias("channel_len_change_per_second_channel_len_to_sec"),
        F.percentile_approx("channel_len_share_of_channel_len_agent_id", 0.5).alias("median_channel_len_share_of_channel_len_agent_id"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

scored = (
    rollup_base
    .select(
        "second_channel_len",
        "closed_at_day",
        "second_channel_len_filled",
        "channel_len_change_per_second_channel_len_to_sec",
        "median_channel_len_share_of_channel_len_agent_id",
        "distinct_ids",
        F.abs(F.col("distinct_ids") - 7).alias("distinct_ids_distance"),
        F.coalesce(F.col("second_channel_len"), F.lit(0)).alias("second_channel_len_filled_2"),
        F.date_format(F.col("closed_at_day"), "yyyy-MM-dd").alias("closed_at_day_label"),
    )
)

result = (
    scored
    .filter(F.col("second_channel_len_filled_2") <= 12)
    .select(
        "second_channel_len",
        "closed_at_day",
        "second_channel_len_filled",
        "distinct_ids",
        "distinct_ids_distance",
        "second_channel_len_filled_2",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("closed_at_day")).alias("days_since_closed_at_day"),
        F.substring(F.trim(F.col("closed_at_day_label")), 1, 4).alias("closed_at_day_label_prefix"),
    )
    .orderBy(F.col("closed_at_day").asc())
)
