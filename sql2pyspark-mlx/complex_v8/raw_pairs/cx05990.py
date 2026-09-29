from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_opened_at_month_closed_at_to = Window.partitionBy(F.date_trunc("month", "opened_at"), F.to_date("closed_at"))
w_channel_by_csat_per_requeste = Window.partitionBy("channel").orderBy(F.col("csat_per_requester_id").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_channel_by_csat_per_requeste_2 = Window.partitionBy("channel").orderBy(F.col("csat_per_requester_id").asc_nulls_last())
w_agent_id = Window.partitionBy("agent_id")
w_distinct_channel_in_opened_a = Window.partitionBy("distinct_channel_in_opened_at_month_closed_at_to").orderBy("csat_per_requester_id_rank")
w_agent_id_by_closed_at = Window.partitionBy("agent_id").orderBy(F.col("closed_at").asc_nulls_last(), "id")

valid = (
    df_tickets
    .filter((F.col("channel") != "chat") & (~F.array_contains(F.col("tags"), "bug")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("closed_at")).alias("days_since_closed_at"),
        F.round(F.col("csat") / F.nullif(F.col("requester_id"), F.lit(0)), 3).alias("csat_per_requester_id"),
    )
)

derived = (
    valid
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("channel")), " ", "_")).alias("channel_slug"),
        F.greatest(F.col("csat"), F.col("requester_id")).alias("greatest_csat_requester_id"),
    )
)

sequenced = (
    derived
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "tags",
        "csat_per_requester_id",
        "greatest_csat_requester_id",
        F.min("csat_per_requester_id").over(w_opened_at_month_closed_at_to).alias("opened_at_month_closed_at_to_date_min_csat_per_r"),
        F.size(F.collect_set("priority").over(w_opened_at_month_closed_at_to)).alias("distinct_priority_in_opened_at_month_closed_at_t"),
        F.size(F.collect_set("channel").over(w_opened_at_month_closed_at_to)).alias("distinct_channel_in_opened_at_month_closed_at_to"),
        F.last("greatest_csat_requester_id", ignorenulls=True).over(w_channel_by_csat_per_requeste).alias("last_known_greatest_csat_requester_id"),
        F.rank().over(w_channel_by_csat_per_requeste_2).alias("csat_per_requester_id_rank"),
    )
)

windowed = (
    sequenced
    .select(
        "*",
        F.count(F.lit(1)).over(w_agent_id).alias("rows_in_agent_id"),
        F.dense_rank().over(w_distinct_channel_in_opened_a).alias("csat_per_requester_id_rank_dense_rank"),
        F.lead("requester_id").over(w_agent_id_by_closed_at).alias("next_requester_id"),
        F.min("greatest_csat_requester_id").over(w_agent_id).alias("agent_id_min_greatest_csat_requester_id"),
        (F.col("csat_per_requester_id") - F.lag("csat_per_requester_id").over(w_agent_id_by_closed_at)).alias("csat_per_requester_id_change"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_agent_id), 2).alias("requester_id_vs_agent_id_avg"),
    )
)

result = (
    windowed
    .groupingSets(
        [["csat_per_requester_id_rank_dense_rank", "distinct_priority_in_opened_at_month_closed_at_t", "distinct_channel_in_opened_at_month_closed_at_to"], ["csat_per_requester_id_rank_dense_rank"], []],
        "csat_per_requester_id_rank_dense_rank", "distinct_priority_in_opened_at_month_closed_at_t", "distinct_channel_in_opened_at_month_closed_at_to",
    )
    .agg(
        F.grouping("csat_per_requester_id_rank_dense_rank").alias("is_total_csat_per_requester_id_rank_dense_rank"),
        F.min("closed_at").alias("first_closed_at"),
        F.max("closed_at").alias("last_closed_at"),
        F.round(F.avg("distinct_channel_in_opened_at_month_closed_at_to"), 2).alias("avg_distinct_channel_in_opened_at_month_closed_a"),
    )
    .orderBy(
        F.col("csat_per_requester_id_rank_dense_rank").asc_nulls_last(),
        F.col("distinct_priority_in_opened_at_month_closed_at_t").asc_nulls_last(),
        F.col("distinct_channel_in_opened_at_month_closed_at_to").asc_nulls_last(),
    )
)
