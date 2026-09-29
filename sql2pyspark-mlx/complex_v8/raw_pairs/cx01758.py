from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_closed_at_to_date = Window.partitionBy(F.to_date("closed_at"))
w_requester_id_tier_closed_at = Window.partitionBy("requester_id_tier", F.date_trunc("month", "closed_at")).orderBy("requester_id", "id")
w_requester_id_tier_closed_at_2 = Window.partitionBy("requester_id_tier", F.date_trunc("month", "closed_at")).orderBy("requester_id", "id").rowsBetween(-6, -1)
w_requester_id_tier_closed_at_3 = Window.partitionBy("requester_id_tier", F.date_trunc("month", "closed_at")).orderBy("requester_id", "id").rowsBetween(-1, Window.currentRow)

valid = (
    df_tickets
    .filter(F.size(F.col("tags")) >= 2)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        F.array_join(F.array_sort(F.col("tags")), ",").alias("tags_text"),
        F.when(F.col("requester_id") >= 18, "HIGH").when(F.col("requester_id") >= 7, "MEDIUM").otherwise("LOW").alias("requester_id_tier"),
        F.when(F.col("channel").isin("email", "phone"), "group_a").when(F.col("channel") == "chat", "group_b").otherwise("other").alias("channel_group"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.min("event_at").alias("first_event_at_2"),
    )
)

summarized_rows = (
    valid.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select("s.*", "t.first_event_at", "t.first_event_at_2")
)

with_trends = (
    summarized_rows
    .select(
        "*",
        F.round(F.col("csat") - F.avg("csat").over(w_closed_at_to_date), 2).alias("csat_vs_closed_at_to_date_avg"),
        F.row_number().over(w_requester_id_tier_closed_at).alias("requester_id_seq"),
        F.size(F.collect_set("priority").over(w_closed_at_to_date)).alias("distinct_priority_in_closed_at_to_date"),
        F.max("requester_id").over(w_requester_id_tier_closed_at_2).alias("requester_id_max_6_prior"),
        F.lag("first_event_at", 3).over(w_requester_id_tier_closed_at).alias("prev3_first_event_at"),
        F.sum("csat").over(w_requester_id_tier_closed_at_3).alias("csat_sum_2"),
    )
)

result = (
    with_trends
    .cube("csat", "requester_id_tier")
    .agg(
        F.array_sort(F.collect_set("channel")).alias("channel_list"),
        F.min("first_event_at").alias("first_first_event_at"),
    )
    .select(
        "csat",
        F.coalesce(F.col("requester_id_tier"), F.lit("ALL")).alias("requester_id_tier"),
        "channel_list",
        "first_first_event_at",
    )
    .orderBy(
        F.col("csat").asc_nulls_last(),
        F.col("requester_id_tier").asc_nulls_last(),
    )
)
