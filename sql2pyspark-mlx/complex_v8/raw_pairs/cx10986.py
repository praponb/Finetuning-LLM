from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_priority = Window.partitionBy("agent_id", "priority")
w_ticket_count_by_requester_id = Window.partitionBy("ticket_count").orderBy("requester_id_filled", "id")
w_ticket_count_by_requester_id_2 = Window.partitionBy("ticket_count").orderBy("requester_id_filled", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_count_by_requester_id_3 = Window.partitionBy("ticket_count").orderBy("requester_id_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_second_ticket_count = Window.partitionBy("second_ticket_count")
w_all_by_requester_id = Window.orderBy(F.col("requester_id").desc(), "id")
w_all_by_requester_id_full = Window.orderBy(F.col("requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_rn_by_first_known_ticket_cou = Window.partitionBy("rn").orderBy(F.col("first_known_ticket_count").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared = (
    df_tickets
    .filter((F.col("priority") != "P2") | (F.col("requester_id") <= 19))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "tags",
        F.coalesce(F.col("requester_id"), F.lit(0)).alias("requester_id_filled"),
        F.concat_ws("|", F.col("priority"), F.col("channel")).alias("priority_channel_label"),
        F.concat(F.col("channel"), F.lit("#"), F.col("csat").cast("string")).alias("channel_ref"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count("*").alias("ticket_count"),
        F.count(F.when(F.col("priority").isin("P3", "P4"), 1)).alias("ticket_priority_count"),
    )
)

with_counts = (
    prepared.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
        F.coalesce(F.col("t.ticket_priority_count"), F.lit(0)).alias("ticket_priority_count"),
    )
)

with_trends = (
    with_counts
    .select(
        "id",
        "requester_id",
        "priority",
        "requester_id_filled",
        "priority_channel_label",
        "channel_ref",
        F.count(F.lit(1)).over(w_agent_id_priority).alias("rows_in_agent_id_priority"),
        F.row_number().over(w_ticket_count_by_requester_id).alias("rn"),
        F.nth_value("ticket_count", 2).over(w_ticket_count_by_requester_id_2).alias("second_ticket_count"),
        F.first("ticket_count", ignorenulls=True).over(w_ticket_count_by_requester_id_3).alias("first_known_ticket_count"),
    )
)

with_windows = (
    with_trends
    .select(
        "*",
        F.max("second_ticket_count").over(w_second_ticket_count).alias("second_ticket_count_max_second_ticket_count"),
        F.lead("requester_id").over(w_all_by_requester_id).alias("next_requester_id"),
        F.last("rn").over(w_all_by_requester_id_full).alias("last_rn"),
        F.min("requester_id").over(w_second_ticket_count).alias("second_ticket_count_min_requester_id"),
        F.nth_value("rn", 2).over(w_rn_by_first_known_ticket_cou).alias("second_rn"),
        F.size(F.collect_set("priority").over(w_second_ticket_count)).alias("distinct_priority_in_second_ticket_count"),
    )
)

result = (
    with_windows
    .cube("first_known_ticket_count", "rn", "priority")
    .agg(
        F.grouping_id().alias("gid"),
        F.array_sort(F.collect_set("priority")).alias("priority_list"),
        F.countDistinct("priority").alias("distinct_prioritys"),
    )
    .select(
        "first_known_ticket_count",
        "rn",
        F.coalesce(F.col("priority"), F.lit("ALL")).alias("priority"),
        "gid",
        "priority_list",
        "distinct_prioritys",
    )
    .orderBy(
        F.col("first_known_ticket_count").asc_nulls_last(),
        F.col("rn").asc_nulls_last(),
        F.col("priority").asc_nulls_last(),
    )
)
