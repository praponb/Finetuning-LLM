from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_days_since_opened_at = Window.orderBy(F.col("days_since_opened_at").desc(), "id").rowsBetween(-6, -1)
w_all_by_days_since_opened_at_2 = Window.orderBy(F.col("days_since_opened_at").desc())
w_all_by_days_since_opened_at_3 = Window.orderBy(F.col("days_since_opened_at").desc(), "id")
w_csat_agent_id = Window.partitionBy("csat", "agent_id")

recent_tickets = (
    df_tickets
    .filter(F.col("channel").isin("phone", "chat", "web"))
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
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("opened_at")).alias("days_since_opened_at"),
    )
)

derived = (
    recent_tickets
    .select(
        "*",
        F.length(F.trim(F.col("channel"))).alias("channel_len"),
        F.when(F.col("csat") >= 3, "A").when(F.col("csat") >= 2, "B").otherwise("C").alias("csat_tier"),
    )
)

derived_2 = (
    derived
    .filter(F.col("csat_tier").isNotNull())
    .withColumn("days_since_opened_at_per_csat", F.round(F.col("days_since_opened_at") / F.nullif(F.col("csat"), F.lit(0)), 3))
)

windowed = (
    derived_2
    .select(
        "*",
        F.sum("requester_id").over(w_all_by_days_since_opened_at).alias("requester_id_sum_6_prior"),
        F.round(F.cume_dist().over(w_all_by_days_since_opened_at_2), 3).alias("days_since_opened_at_cume_dist"),
        F.ntile(3).over(w_all_by_days_since_opened_at_3).alias("days_since_opened_at_tercile"),
        F.round(F.col("csat") - F.avg("csat").over(w_csat_agent_id), 2).alias("csat_vs_csat_agent_id_avg"),
        F.round(F.percent_rank().over(w_all_by_days_since_opened_at_3), 3).alias("days_since_opened_at_percent_rank"),
        F.round(F.avg("csat").over(w_csat_agent_id), 2).alias("csat_agent_id_avg_csat"),
    )
)

joined_agent = (
    windowed.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select("s.*", F.col("su.hired_on").alias("agent_hired_on"))
)

grouped = (
    joined_agent
    .groupBy("channel_len")
    .agg(
        F.round(F.sum("csat_agent_id_avg_csat") / F.nullif(F.sum("requester_id"), F.lit(0)), 3).alias("csat_agent_id_avg_csat_to_requester_id_ratio"),
        F.round(F.avg("requester_id_sum_6_prior"), 2).alias("avg_requester_id_sum_6_prior"),
        F.round(F.sum("days_since_opened_at_percent_rank"), 2).alias("total_days_since_opened_at_percent_rank"),
        F.sum("days_since_opened_at_tercile").alias("total_days_since_opened_at_tercile"),
    )
)

result = (
    grouped
    .select(
        "channel_len",
        "csat_agent_id_avg_csat_to_requester_id_ratio",
        "avg_requester_id_sum_6_prior",
        "total_days_since_opened_at_percent_rank",
        F.coalesce(F.col("channel_len"), F.lit(0)).alias("channel_len_filled"),
        F.when(F.col("channel_len") >= 9, "A").when(F.col("channel_len") >= 6, "B").otherwise("C").alias("channel_len_tier"),
    )
    .orderBy("channel_len_tier", F.col("channel_len_filled").desc(), "channel_len")
    .limit(10)
)
