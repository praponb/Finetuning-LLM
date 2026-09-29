from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_csat_by_csat = Window.partitionBy("agent_id", "csat").orderBy("csat", "id")
w_priority_agent_id = Window.partitionBy("priority", "agent_id")
w_priority_by_opened_at_runnin = Window.partitionBy("priority").orderBy(F.col("opened_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_id_csat_by_csat_prior = Window.partitionBy("agent_id", "csat").orderBy("csat", "id").rowsBetween(-6, -1)
w_priority_by_opened_at_prior = Window.partitionBy("priority").orderBy(F.col("opened_at").asc_nulls_last(), "id").rowsBetween(-1, -1)
w_agent_tier_channel = Window.partitionBy("agent_tier", "channel")
w_channel_agent_tier = Window.partitionBy("channel", "agent_tier")
w_all_by_minutes_opened_at_to = Window.orderBy("minutes_opened_at_to_closed_at", "id").rowsBetween(-2, Window.currentRow)
w_all_by_closed_at = Window.orderBy(F.col("closed_at").desc(), "id")

base_tickets = df_tickets.filter(~F.col("channel").isin("web", "email"))

with_details = (
    base_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.channel",
        "s.csat",
        "s.tags",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

ranked = (
    with_details
    .select(
        "id",
        "opened_at",
        "closed_at",
        "channel",
        "tags",
        "agent_tier",
        F.lag("requester_id").over(w_agent_id_csat_by_csat).alias("prev_requester_id"),
        F.min("requester_id").over(w_priority_agent_id).alias("priority_agent_id_min_requester_id"),
        F.first("requester_id", ignorenulls=True).over(w_priority_by_opened_at_runnin).alias("first_known_requester_id"),
        F.count(F.lit(1)).over(w_priority_agent_id).alias("rows_in_priority_agent_id"),
        F.sum("csat").over(w_agent_id_csat_by_csat_prior).alias("csat_sum_6_prior"),
        F.round(F.avg("csat").over(w_priority_by_opened_at_prior), 2).alias("csat_avg_1_prior"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_agent_tier_channel), 2).alias("requester_id_vs_agent_tier_channel_avg"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_agent_tier_channel), F.lit(0)), 4).alias("requester_id_share_of_agent_tier_channel"),
    )
)

annotated = (
    ranked
    .select(
        "*",
        F.round((F.unix_timestamp(F.col("closed_at")) - F.unix_timestamp(F.col("opened_at"))) / 60, 1).alias("minutes_opened_at_to_closed_at"),
    )
)

metrics = (
    annotated
    .select(
        "id",
        "opened_at",
        "closed_at",
        "channel",
        "tags",
        "agent_tier",
        "prev_requester_id",
        "priority_agent_id_min_requester_id",
        "rows_in_priority_agent_id",
        "csat_sum_6_prior",
        "requester_id_vs_agent_tier_channel_avg",
        "requester_id_share_of_agent_tier_channel",
        F.round(F.col("prev_requester_id") - F.avg("prev_requester_id").over(w_channel_agent_tier), 2).alias("prev_requester_id_vs_channel_agent_tier_avg"),
        F.round(F.avg("prev_requester_id").over(w_all_by_minutes_opened_at_to), 2).alias("prev_requester_id_avg_3"),
        F.round(F.avg("rows_in_priority_agent_id").over(w_channel_agent_tier), 2).alias("channel_agent_tier_avg_rows_in_priority_agent_id"),
        F.round(F.col("rows_in_priority_agent_id") - F.avg("rows_in_priority_agent_id").over(w_channel_agent_tier), 2).alias("rows_in_priority_agent_id_vs_channel_agent_tier_"),
        F.lag("prev_requester_id", 3).over(w_all_by_closed_at).alias("prev3_prev_requester_id"),
        F.dense_rank().over(w_all_by_closed_at).alias("closed_at_dense_rank"),
    )
)

rollup_base = (
    metrics
    .groupBy(F.date_format("opened_at", "yyyy-MM").alias("opened_at_ym"))
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.stddev("prev_requester_id_avg_3"), 2).alias("stddev_prev_requester_id_avg_3"),
        F.min("opened_at").alias("first_opened_at"),
        F.min("channel_agent_tier_avg_rows_in_priority_agent_id").alias("min_channel_agent_tier_avg_rows_in_priority_agen"),
    )
)

result = (
    rollup_base
    .filter(F.to_date(F.col("first_opened_at")) < F.lit("2025-02-18").cast("date"))
    .select("cnt")
    .distinct()
    .orderBy(F.col("cnt").asc_nulls_last())
)
