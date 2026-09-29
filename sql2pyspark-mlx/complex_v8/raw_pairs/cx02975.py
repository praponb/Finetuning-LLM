from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_closed_at_month = Window.partitionBy("agent_id", F.month("closed_at"))
w_tag_by_closed_at_full = Window.partitionBy("tag").orderBy(F.col("closed_at").desc_nulls_first(), "id", "tag").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_tag_by_closed_at_ties = Window.partitionBy("tag").orderBy(F.col("closed_at").desc_nulls_first())
w_dedup = Window.partitionBy("channel", "agent_id").orderBy(F.col("closed_at").desc(), F.col("id").desc(), F.col("tag").desc())
w_priority_first_csat_by_agent = Window.partitionBy("priority", "first_csat").orderBy("agent_id_closed_at_month_max_hours_opened_at_to_", "id", "tag").rowsBetween(-2, -1)
w_priority_first_csat_by_agent_2 = Window.partitionBy("priority", "first_csat").orderBy("agent_id_closed_at_month_max_hours_opened_at_to_")
w_priority_first_csat_by_agent_3 = Window.partitionBy("priority", "first_csat").orderBy("agent_id_closed_at_month_max_hours_opened_at_to_", "id", "tag").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_priority_first_csat_by_agent_4 = Window.partitionBy("priority", "first_csat").orderBy("agent_id_closed_at_month_max_hours_opened_at_to_", "id", "tag")

filtered_tickets = (
    df_tickets
    .filter((F.col("requester_id") < 23) & (F.col("agent_id").isNotNull()))
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.round((F.unix_timestamp(F.col("closed_at")) - F.unix_timestamp(F.col("opened_at"))) / 3600, 1).alias("hours_opened_at_to_closed_at"),
    )
)

flattened = (
    filtered_tickets
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "hours_opened_at_to_closed_at",
        F.explode("tags").alias("tag"),
    )
)

metrics = (
    flattened
    .select(
        "*",
        F.max("hours_opened_at_to_closed_at").over(w_agent_id_closed_at_month).alias("agent_id_closed_at_month_max_hours_opened_at_to_"),
        F.last("hours_opened_at_to_closed_at").over(w_tag_by_closed_at_full).alias("last_hours_opened_at_to_closed_at"),
        F.round(F.col("hours_opened_at_to_closed_at") / F.nullif(F.sum("hours_opened_at_to_closed_at").over(w_agent_id_closed_at_month), F.lit(0)), 4).alias("hours_opened_at_to_closed_at_share_of_agent_id_c"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_agent_id_closed_at_month), F.lit(0)), 4).alias("csat_share_of_agent_id_closed_at_month"),
        F.dense_rank().over(w_tag_by_closed_at_ties).alias("closed_at_dense_rank"),
        F.first("csat").over(w_tag_by_closed_at_full).alias("first_csat"),
    )
)

top_rows = (
    metrics
    .filter((F.col("closed_at_dense_rank") == 1) & (F.col("first_csat") > 2))
    .drop("closed_at_dense_rank")
)

latest_per_key = (
    top_rows
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

annotated = (
    latest_per_key
    .filter(F.col("csat_share_of_agent_id_closed_at_month") <= 0.8)
    .withColumn("channel_len", F.length(F.trim(F.col("channel"))))
)

ranked = (
    annotated
    .select(
        "*",
        F.round(F.avg("hours_opened_at_to_closed_at").over(w_priority_first_csat_by_agent), 2).alias("hours_opened_at_to_closed_at_avg_2_prior"),
        F.round(F.percent_rank().over(w_priority_first_csat_by_agent_2), 3).alias("agent_id_closed_at_month_max_hours_opened_at_to__2"),
        F.nth_value("closed_at", 2).over(w_priority_first_csat_by_agent_3).alias("second_closed_at"),
        F.ntile(10).over(w_priority_first_csat_by_agent_4).alias("agent_id_closed_at_month_max_hours_opened_at_to__3"),
        F.dense_rank().over(w_priority_first_csat_by_agent_2).alias("agent_id_closed_at_month_max_hours_opened_at_to__4"),
        F.lag("channel_len", 3, 0).over(w_priority_first_csat_by_agent_4).alias("prev3_channel_len"),
    )
)

result = (
    ranked
    .rollup("first_csat", "agent_id")
    .agg(
        F.array_sort(F.collect_set("channel")).alias("channel_list"),
        F.count("*").alias("n_rows"),
        F.countDistinct("priority").alias("distinct_prioritys"),
    )
)
