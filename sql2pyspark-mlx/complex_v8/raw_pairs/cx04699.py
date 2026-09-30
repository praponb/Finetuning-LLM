from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("agent_id", "channel").orderBy(F.col("csat").desc(), F.col("id").desc())
w_all_by_csat_moving_6 = Window.orderBy("csat", "id").rowsBetween(-6, Window.currentRow)
w_opened_at_month_channel = Window.partitionBy(F.date_trunc("month", "opened_at"), "channel")
w_all_by_csat_moving_2 = Window.orderBy("csat", "id").rowsBetween(-2, Window.currentRow)
w_all_by_csat_full = Window.orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_opened_at_year_by_tags_count = Window.partitionBy(F.year("opened_at")).orderBy(F.col("tags_count").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_csat_opened_at_month_by_firs = Window.partitionBy("csat", F.date_trunc("month", "opened_at")).orderBy(F.col("first_least_requester_id_csat").desc())
w_tags_count_by_first_least_re = Window.partitionBy("tags_count").orderBy(F.col("first_least_requester_id_csat").desc())
w_tags_count = Window.partitionBy("tags_count")
w_dedup = Window.partitionBy("priority").orderBy(F.col("least_requester_id_csat").desc(), F.col("id").desc())

scoped = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.size(F.col("tags")).alias("tags_count"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("opened_at")).alias("days_since_opened_at"),
        F.least(F.col("requester_id"), F.col("csat")).alias("least_requester_id_csat"),
    )
)

latest_per_key = (
    scoped
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

metrics = (
    latest_per_key
    .select(
        "*",
        F.round(F.avg("tags_count").over(w_all_by_csat_moving_6), 2).alias("tags_count_avg_7"),
        F.size(F.collect_set("priority").over(w_opened_at_month_channel)).alias("distinct_priority_in_opened_at_month_channel"),
        F.round(F.avg("requester_id").over(w_all_by_csat_moving_2), 2).alias("requester_id_avg_3"),
        F.nth_value("csat", 2).over(w_all_by_csat_full).alias("second_csat"),
        F.first("least_requester_id_csat").over(w_opened_at_year_by_tags_count).alias("first_least_requester_id_csat"),
    )
)

channel_stats = (
    metrics
    .groupBy("channel")
    .agg(F.avg("days_since_opened_at").alias("avg_days_since_opened_at"))
)

outliers = (
    metrics.alias("s")
    .join(channel_stats.alias("g"), F.col("g.channel") == F.col("s.channel"), "inner")
    .filter(F.col("s.days_since_opened_at") > F.col("g.avg_days_since_opened_at"))
    .select(
        "s.*",
        "g.avg_days_since_opened_at",
        F.round(F.col("s.days_since_opened_at") - F.col("g.avg_days_since_opened_at"), 2).alias("days_since_opened_at_minus_avg"),
    )
)

metrics_2 = (
    outliers
    .select(
        "*",
        F.round(F.percent_rank().over(w_csat_opened_at_month_by_firs), 3).alias("first_least_requester_id_csat_percent_rank"),
        F.rank().over(w_tags_count_by_first_least_re).alias("first_least_requester_id_csat_rank"),
        F.rank().over(w_csat_opened_at_month_by_firs).alias("first_least_requester_id_csat_rank_2"),
        F.round(F.col("csat") - F.avg("csat").over(w_tags_count), 2).alias("csat_vs_tags_count_avg"),
    )
)

unique_rows = (
    metrics_2
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    unique_rows
    .filter(F.col("first_least_requester_id_csat_percent_rank") < 0.8)
    .select("tags_count", "avg_days_since_opened_at", "tags_count_avg_7")
    .distinct()
    .orderBy(F.col("avg_days_since_opened_at").asc(), "tags_count_avg_7")
)
