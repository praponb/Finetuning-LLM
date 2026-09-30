from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_agent_id_by_request = Window.partitionBy("priority", "agent_id").orderBy(F.col("requester_id").desc(), "id").rowsBetween(-2, -1)
w_priority_tags_count = Window.partitionBy("priority", "tags_count")
w_tags_count_closed_at_to_date = Window.partitionBy("tags_count", F.to_date("closed_at")).orderBy(F.col("closed_at").desc(), "id")

prepared_tickets = (
    df_tickets
    .filter(F.col("csat") <= 4)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.concat(F.col("channel"), F.lit("#"), F.col("id").cast("string")).alias("channel_ref"),
        F.substring(F.trim(F.col("channel")), 1, 3).alias("channel_prefix"),
        F.size(F.col("tags")).alias("tags_count"),
    )
)

with_trends = (
    prepared_tickets
    .select(
        "*",
        F.sum("csat").over(w_priority_agent_id_by_request).alias("csat_sum_2_prior"),
        F.round(F.avg("csat").over(w_priority_tags_count), 2).alias("priority_tags_count_avg_csat"),
        F.ntile(5).over(w_tags_count_closed_at_to_date).alias("closed_at_quintile"),
        F.size(F.collect_set("agent_id").over(w_priority_tags_count)).alias("distinct_agent_id_in_priority_tags_count"),
    )
)

totals = (
    with_trends
    .filter(F.col("agent_id").isNotNull())
    .groupBy(F.date_trunc("month", "closed_at").alias("closed_at_month"))
    .agg(
        F.array_sort(F.collect_set("channel")).alias("channel_list"),
        F.round(F.avg("tags_count"), 2).alias("avg_tags_count"),
        F.round(100 * F.count(F.when(F.col("channel").isNotNull(), 1)) / F.count("*"), 1).alias("pct_channel_is_not_null"),
        F.sum(F.when(F.col("channel").isin("web", "chat", "phone"), 1).otherwise(0)).alias("n_channel_in_web_chat_phone"),
        F.max("priority_tags_count_avg_csat").alias("max_priority_tags_count_avg_csat_check"),
    )
    .filter(F.col("max_priority_tags_count_avg_csat_check") > 1.6)
    .drop("max_priority_tags_count_avg_csat_check")
)

result = (
    totals
    .select(
        "closed_at_month",
        "channel_list",
        "avg_tags_count",
        "pct_channel_is_not_null",
        "n_channel_in_web_chat_phone",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("closed_at_month")).alias("days_since_closed_at_month"),
        F.when(F.col("avg_tags_count") >= 2.1, "A").when(F.col("avg_tags_count") >= 0.6, "B").otherwise("C").alias("avg_tags_count_tier"),
    )
)
