from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_opened_at_month_channel = Window.partitionBy(F.date_trunc("month", "opened_at"), "channel")
w_all_by_opened_at_prior_1 = Window.orderBy(F.col("opened_at").desc(), "id").rowsBetween(-1, -1)
w_latest = Window.partitionBy("priority", "channel").orderBy(F.col("csat_sum_1_prior").desc(), F.col("id").desc())

cleaned_tickets = (
    df_tickets
    .filter((F.col("priority") == "P2") & (F.array_contains(F.col("tags"), "outage")))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
    )
)

windowed = (
    cleaned_tickets
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        F.size(F.collect_set("agent_id").over(w_opened_at_month_channel)).alias("distinct_agent_id_in_opened_at_month_channel"),
        F.sum("csat").over(w_all_by_opened_at_prior_1).alias("csat_sum_1_prior"),
        F.max("csat").over(w_opened_at_month_channel).alias("opened_at_month_channel_max_csat"),
    )
)

latest_per_key = (
    windowed
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

annotated = (
    latest_per_key
    .filter(F.col("priority") == "P1")
    .select(
        "*",
        (F.col("closed_at") + F.expr("INTERVAL 15 MINUTES")).alias("closed_at_plus_15m"),
        F.concat_ws("-", F.col("priority"), F.col("channel")).alias("priority_channel_label"),
        F.substring(F.trim(F.col("priority")), 1, 3).alias("priority_prefix"),
    )
)

result = (
    annotated
    .cube("priority", "distinct_agent_id_in_opened_at_month_channel")
    .agg(
        F.grouping_id().alias("gid"),
        F.percentile_approx("opened_at_month_channel_max_csat", 0.5).alias("median_opened_at_month_channel_max_csat"),
        F.count("*").alias("n_rows"),
        F.round(F.avg("opened_at_month_channel_max_csat"), 2).alias("avg_opened_at_month_channel_max_csat"),
    )
    .select(
        F.coalesce(F.col("priority"), F.lit("ALL")).alias("priority"),
        "distinct_agent_id_in_opened_at_month_channel",
        "gid",
        "median_opened_at_month_channel_max_csat",
        "n_rows",
        "avg_opened_at_month_channel_max_csat",
    )
    .orderBy(
        F.col("priority").asc_nulls_last(),
        F.col("distinct_agent_id_in_opened_at_month_channel").asc_nulls_last(),
    )
)
