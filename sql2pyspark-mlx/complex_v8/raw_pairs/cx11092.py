from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_hired_on_month_by_clos = Window.partitionBy(F.date_trunc("month", "agent_hired_on")).orderBy("closed_at", "id")
w_agent_hired_on_year_channel = Window.partitionBy(F.year("agent_hired_on"), "channel_group").orderBy(F.col("agent_hired_on").desc())
w_agent_hired_on_year_channel_2 = Window.partitionBy(F.year("agent_hired_on"), "channel_group").orderBy(F.col("agent_hired_on").desc(), "id")
w_agent_id_by_closed_at_runnin = Window.partitionBy("agent_id").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_id_by_closed_at = Window.partitionBy("agent_id").orderBy("closed_at", "id")
w_agent_id_priority_by_closed = Window.partitionBy("agent_id", "priority").orderBy("closed_at", "id")
w_channel_group_by_closed_at = Window.partitionBy("channel_group").orderBy(F.col("closed_at").desc(), "id")
w_next2_closed_at_year = Window.partitionBy("next2_closed_at_year")

recent_tickets = (
    df_tickets
    .filter((F.col("closed_at") >= F.lit("2025-02-08 13:00:00").cast("timestamp")) | (F.col("agent_id") >= 5))
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "tags",
        F.coalesce(F.col("channel"), F.lit("UNKNOWN")).alias("channel_or_unknown"),
        F.when(F.col("channel").isin("web", "email"), "group_a").when(F.col("channel") == "chat", "group_b").otherwise("other").alias("channel_group"),
    )
)

combined_details_agent = (
    recent_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.priority",
        "s.tags",
        "s.channel_group",
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

with_trends = (
    combined_details_agent
    .select(
        "*",
        F.ntile(10).over(w_agent_hired_on_month_by_clos).alias("closed_at_decile"),
        F.round(F.percent_rank().over(w_agent_hired_on_year_channel), 3).alias("agent_hired_on_percent_rank"),
        F.lag("closed_at").over(w_agent_hired_on_year_channel_2).alias("prev_closed_at"),
        F.dense_rank().over(w_agent_hired_on_year_channel).alias("agent_hired_on_dense_rank"),
        F.last("agent_hired_on", ignorenulls=True).over(w_agent_id_by_closed_at_runnin).alias("last_known_agent_hired_on"),
        F.row_number().over(w_agent_hired_on_month_by_clos).alias("seq_no"),
        F.dense_rank().over(w_agent_id_by_closed_at).alias("closed_at_dense_rank"),
        F.datediff(F.col("agent_hired_on"), F.lag("agent_hired_on").over(w_agent_hired_on_month_by_clos)).alias("days_since_prev_agent_hired_on"),
        F.lead("closed_at", 2).over(w_agent_id_priority_by_closed).alias("next2_closed_at"),
        F.lag("closed_at", 3).over(w_channel_group_by_closed_at).alias("prev3_closed_at"),
        F.dense_rank().over(w_agent_id_priority_by_closed).alias("closed_at_dense_rank_2"),
    )
)

rollup_base = (
    with_trends
    .groupBy(
        "closed_at_dense_rank",
        F.year("next2_closed_at").alias("next2_closed_at_year"),
        F.date_format("agent_hired_on", "yyyy-MM").alias("agent_hired_on_ym"),
    )
    .agg(
        F.array_sort(F.collect_set("priority")).alias("priority_list"),
        F.max("closed_at_decile").alias("max_closed_at_decile_check"),
    )
    .filter(F.col("max_closed_at_decile_check") > 2)
    .drop("max_closed_at_decile_check")
)

with_trends_2 = (
    rollup_base
    .select(
        "*",
        F.max("next2_closed_at_year").over(w_next2_closed_at_year).alias("next2_closed_at_year_max_next2_closed_at_year"),
        F.min("next2_closed_at_year").over(w_next2_closed_at_year).alias("next2_closed_at_year_min_next2_closed_at_year"),
        F.max("closed_at_dense_rank").over(w_next2_closed_at_year).alias("next2_closed_at_year_max_closed_at_dense_rank"),
        F.round(F.avg("next2_closed_at_year").over(w_next2_closed_at_year), 2).alias("next2_closed_at_year_avg_next2_closed_at_year"),
    )
)

result = (
    with_trends_2
    .select(
        "closed_at_dense_rank",
        "next2_closed_at_year",
        "agent_hired_on_ym",
        "priority_list",
        "next2_closed_at_year_min_next2_closed_at_year",
        "next2_closed_at_year_max_closed_at_dense_rank",
        "next2_closed_at_year_avg_next2_closed_at_year",
        (F.floor(F.col("closed_at_dense_rank") / 2) * 2).alias("closed_at_dense_rank_bucket"),
        F.when(F.col("next2_closed_at_year") >= 2025, "HIGH").when(F.col("next2_closed_at_year") >= 2024, "MEDIUM").otherwise("LOW").alias("next2_closed_at_year_tier"),
    )
    .orderBy(
        F.col("next2_closed_at_year_min_next2_closed_at_year").desc(),
        "closed_at_dense_rank",
        "next2_closed_at_year",
        "agent_hired_on_ym",
    )
    .limit(5)
)
