from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_hired_on_year_channel = Window.partitionBy(F.year("agent_hired_on"), "channel")
w_all_by_greatest_csat_request = Window.orderBy("greatest_csat_requester_id", "id").rowsBetween(-1, Window.currentRow)

recent_tickets = (
    df_tickets
    .filter(F.col("csat") <= 4)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "channel",
        F.to_date(F.col("opened_at")).alias("opened_at_date"),
        F.greatest(F.col("csat"), F.col("requester_id")).alias("greatest_csat_requester_id"),
        F.length(F.trim(F.col("channel"))).alias("channel_len"),
    )
)

joined = (
    recent_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.channel",
        "s.opened_at_date",
        "s.greatest_csat_requester_id",
        "s.channel_len",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier"),
    )
)

with_windows = (
    joined
    .select(
        "*",
        F.size(F.collect_set("agent_tier").over(w_agent_hired_on_year_channel)).alias("distinct_agent_tier_in_agent_hired_on_year_chann"),
        F.min("requester_id").over(w_agent_hired_on_year_channel).alias("agent_hired_on_year_channel_min_requester_id"),
        F.round(F.avg("channel_len").over(w_all_by_greatest_csat_request), 2).alias("channel_len_avg_2"),
        F.round(F.col("greatest_csat_requester_id") - F.avg("greatest_csat_requester_id").over(w_agent_hired_on_year_channel), 2).alias("greatest_csat_requester_id_vs_agent_hired_on_yea"),
        F.count(F.lit(1)).over(w_agent_hired_on_year_channel).alias("rows_in_agent_hired_on_year_channel"),
    )
)

joined_agent = (
    with_windows.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at_date",
        "s.greatest_csat_requester_id",
        "s.channel_len",
        "s.agent_hired_on",
        "s.agent_tier",
        "s.agent_hired_on_year_channel_min_requester_id",
        "s.rows_in_agent_hired_on_year_channel",
        F.col("su.hired_on").alias("agent_hired_on_2"),
    )
)

stats = (
    joined_agent
    .groupBy(
        F.date_trunc("month", "agent_hired_on_2").alias("agent_hired_on_2_month"),
    )
    .agg(
        F.max("agent_hired_on_year_channel_min_requester_id").alias("max_agent_hired_on_year_channel_min_requester_id"),
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("agent_tier") == "T3", 1)).alias("n_agent_tier_t3"),
        F.max("rows_in_agent_hired_on_year_channel").alias("max_rows_in_agent_hired_on_year_channel_check"),
    )
    .filter(F.col("max_rows_in_agent_hired_on_year_channel_check") > 8)
    .drop("max_rows_in_agent_hired_on_year_channel_check")
)

derived = (
    stats
    .select(
        "agent_hired_on_2_month",
        "max_agent_hired_on_year_channel_min_requester_id",
        "row_count",
        "n_agent_tier_t3",
        F.abs(F.col("row_count") - 5).alias("row_count_distance"),
    )
)

result = (
    derived
    .select(
        "agent_hired_on_2_month",
        "row_count",
        "n_agent_tier_t3",
        "row_count_distance",
        "max_agent_hired_on_year_channel_min_requester_id",
    )
    .unpivot(
        ids=["agent_hired_on_2_month"],
        values=["row_count", "n_agent_tier_t3", "row_count_distance", "max_agent_hired_on_year_channel_min_requester_id"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("agent_hired_on_2_month", "metric")
)
