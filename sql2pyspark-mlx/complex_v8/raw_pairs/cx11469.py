from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_requester_id_ties = Window.orderBy("requester_id")
w_all_by_csat_filled_full = Window.orderBy(F.col("csat_filled").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_csat_filled_running = Window.orderBy(F.col("csat_filled").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_csat_opened_at_month = Window.partitionBy("csat", F.month("opened_at"))
w_all_by_requester_id = Window.orderBy("requester_id", "id")
w_distinct_event_types = Window.partitionBy("distinct_event_types")
w_all_by_distinct_event_types = Window.orderBy(F.col("distinct_event_types").desc(), "priority").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_priority_by_total_requester = Window.partitionBy("priority").orderBy("total_requester_id_cume_dist", "priority")

valid = (
    df_tickets
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
        F.coalesce(F.col("csat"), F.lit(0)).alias("csat_filled"),
        F.substring(F.trim(F.col("channel")), 1, 4).alias("channel_prefix"),
        F.concat(F.col("priority"), F.lit("#"), F.col("requester_id").cast("string")).alias("priority_ref"),
    )
)

windowed = (
    valid
    .select(
        "*",
        F.dense_rank().over(w_all_by_requester_id_ties).alias("requester_id_dense_rank"),
        F.first("csat_filled").over(w_all_by_csat_filled_full).alias("first_csat_filled"),
        F.sum("csat").over(w_all_by_csat_filled_running).alias("running_csat"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_csat_opened_at_month), F.lit(0)), 4).alias("requester_id_share_of_csat_opened_at_month"),
        F.round(F.cume_dist().over(w_all_by_requester_id), 3).alias("requester_id_cume_dist"),
    )
)

exploded_detail = (
    windowed.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.priority",
        "s.channel",
        "s.csat_filled",
        "s.channel_prefix",
        "s.first_csat_filled",
        "s.requester_id_cume_dist",
        F.col("t.id").alias("event_id"),
        "t.event_type",
        F.col("t.actor").alias("event_actor"),
        "t.event_at",
    )
)

summary = (
    exploded_detail
    .groupBy("priority")
    .agg(
        F.round(F.sum("requester_id_cume_dist"), 2).alias("total_requester_id_cume_dist"),
        F.array_sort(F.collect_set("priority")).alias("priority_list"),
        F.countDistinct("event_type").alias("distinct_event_types"),
    )
)

with_windows = (
    summary
    .select(
        "*",
        F.size(F.collect_set("priority").over(w_distinct_event_types)).alias("distinct_priority_in_distinct_event_types"),
        F.round(F.col("distinct_event_types") / F.nullif(F.sum("distinct_event_types").over(w_distinct_event_types), F.lit(0)), 4).alias("distinct_event_types_share_of_distinct_event_typ"),
        F.nth_value("total_requester_id_cume_dist", 2).over(w_all_by_distinct_event_types).alias("second_total_requester_id_cume_dist"),
        F.round(F.cume_dist().over(w_priority_by_total_requester), 3).alias("total_requester_id_cume_dist_cume_dist"),
    )
)

annotated = (
    with_windows
    .filter(F.col("second_total_requester_id_cume_dist") >= 1.5)
    .select(
        "*",
        F.when(F.col("priority").isin("P4", "P2"), "group_a").when(F.col("priority") == "P1", "group_b").otherwise("other").alias("priority_group"),
        F.lower(F.regexp_replace(F.trim(F.col("priority")), " ", "_")).alias("priority_slug"),
    )
)

result = (
    annotated
    .select(
        "priority",
        "total_requester_id_cume_dist",
        "priority_list",
        "distinct_priority_in_distinct_event_types",
        "distinct_event_types_share_of_distinct_event_typ",
        "second_total_requester_id_cume_dist",
        "total_requester_id_cume_dist_cume_dist",
        "priority_group",
        "priority_slug",
        F.concat_ws("-", F.col("priority_group"), F.col("priority")).alias("priority_group_priority_label"),
        (F.floor(F.col("distinct_event_types") / 2) * 2).alias("distinct_event_types_bucket"),
    )
)
