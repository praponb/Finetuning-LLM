from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("priority").orderBy(F.col("opened_at").desc(), F.col("id").desc())
w_agent_id = Window.partitionBy("agent_id")
w_agent_id_priority_by_request = Window.partitionBy("agent_id", "priority").orderBy(F.col("requester_id").desc(), "id").rowsBetween(-3, Window.currentRow)
w_all_by_requester_id = Window.orderBy(F.col("requester_id").desc_nulls_first(), "id")
w_channel_by_requester_id_prio = Window.partitionBy("channel").orderBy(F.col("requester_id").desc(), "id").rowsBetween(-6, -1)
w_agent_id_priority_by_request_2 = Window.partitionBy("agent_id", "priority").orderBy(F.col("requester_id").desc(), "id")

filtered_tickets = (
    df_tickets
    .filter(F.col("opened_at") >= F.lit("2025-01-19 07:00:00").cast("timestamp"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        F.lower(F.regexp_replace(F.trim(F.col("priority")), " ", "_")).alias("priority_slug"),
        F.concat_ws("-", F.col("priority"), F.col("channel")).alias("priority_channel_label"),
    )
)

latest_per_key = (
    filtered_tickets
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

ranked = (
    latest_per_key
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority_slug",
        F.size(F.collect_set("channel").over(w_agent_id)).alias("distinct_channel_in_agent_id"),
        F.round(F.avg("requester_id").over(w_agent_id_priority_by_request), 2).alias("requester_id_avg_4"),
        F.round((F.unix_timestamp("opened_at") - F.unix_timestamp(F.lag("opened_at").over(w_all_by_requester_id))) / 60, 1).alias("minutes_since_prev_opened_at"),
        F.max("requester_id").over(w_channel_by_requester_id_prio).alias("requester_id_max_6_prior"),
        F.rank().over(w_agent_id_priority_by_request_2).alias("requester_id_rank"),
        F.ntile(5).over(w_agent_id_priority_by_request_2).alias("requester_id_quintile"),
    )
)

first_rows = ranked.filter((F.col("requester_id_rank") == 1) & (F.col("agent_id").isNotNull()))

rollup_base = (
    first_rows
    .groupBy("distinct_channel_in_agent_id", "requester_id_quintile")
    .agg(
        F.max("opened_at").alias("last_opened_at"),
        F.round(F.sum("requester_id_avg_4") / F.nullif(F.sum("requester_id"), F.lit(0)), 3).alias("requester_id_avg_4_to_requester_id_ratio"),
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum("requester_id_quintile").alias("total_requester_id_quintile"),
    )
)

derived = (
    rollup_base
    .select(
        "*",
        F.date_trunc("hour", F.col("last_opened_at")).alias("last_opened_at_hour"),
        F.dayofweek(F.col("last_opened_at")).alias("last_opened_at_dayofweek"),
        (F.floor(F.col("requester_id_quintile") / 2) * 2).alias("requester_id_quintile_bucket"),
    )
)

filtered = (
    derived
    .crossJoin(derived.agg(F.min("row_count").alias("overall_min_row_count")))
    .filter(F.col("row_count") > F.col("overall_min_row_count"))
    .drop("overall_min_row_count")
)

result = (
    filtered
    .filter(F.col("last_opened_at") < F.lit("2025-02-12 10:00:00").cast("timestamp"))
    .select(
        "distinct_channel_in_agent_id",
        "requester_id_quintile",
        "last_opened_at",
        "row_count",
        "last_opened_at_hour",
        "last_opened_at_dayofweek",
        "requester_id_quintile_bucket",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("last_opened_at_hour")).alias("days_since_last_opened_at_hour"),
        F.date_trunc("month", F.col("last_opened_at_hour")).alias("last_opened_at_hour_month"),
    )
    .orderBy("days_since_last_opened_at_hour")
)
