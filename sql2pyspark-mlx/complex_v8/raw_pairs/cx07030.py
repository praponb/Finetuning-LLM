from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_opened_at_month = Window.partitionBy("priority", F.month("opened_at"))
w_csat_by_csat_running = Window.partitionBy("csat").orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_opened_at_month_csat_by_csat = Window.partitionBy(F.month("opened_at"), "csat").orderBy("csat", "id")
w_csat_by_csat_ties = Window.partitionBy("csat").orderBy("csat")
w_prev3_closed_at_to_date_csat = Window.partitionBy(F.to_date("prev3_closed_at"), "csat")
w_csat_rank_by_csat_rank = Window.partitionBy("csat_rank").orderBy(F.col("csat_rank").desc(), "id")
w_all_by_event_actor_count_pri = Window.orderBy(F.col("event_actor_count").desc_nulls_first(), "id").rowsBetween(-2, -1)

cleaned_tickets = (
    df_tickets
    .filter((F.col("requester_id") <= 16) | (F.col("opened_at") < F.lit("2025-03-02 08:00:00").cast("timestamp")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        "tags",
    )
)

tagged = (
    cleaned_tickets
    .select(
        "*",
        (F.floor(F.col("requester_id") / 5) * 5).alias("requester_id_bucket"),
        F.concat(F.col("priority"), F.lit("#"), F.col("requester_id").cast("string")).alias("priority_ref"),
    )
)

windowed = (
    tagged
    .select(
        "*",
        F.sum("csat").over(w_priority_opened_at_month).alias("priority_opened_at_month_sum_csat"),
        F.size(F.collect_set("agent_id").over(w_priority_opened_at_month)).alias("distinct_agent_id_in_priority_opened_at_month"),
        F.last("requester_id_bucket", ignorenulls=True).over(w_csat_by_csat_running).alias("last_known_requester_id_bucket"),
        F.lag("closed_at", 3).over(w_opened_at_month_csat_by_csat).alias("prev3_closed_at"),
        F.count(F.lit(1)).over(w_priority_opened_at_month).alias("rows_in_priority_opened_at_month"),
        F.rank().over(w_csat_by_csat_ties).alias("csat_rank"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count(F.when(F.col("actor") == "agent", 1)).alias("event_actor_count"),
        F.count(F.when(F.col("event_type") != "replied", 1)).alias("event_event_type_count"),
    )
)

with_counts = (
    windowed.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_actor_count"), F.lit(0)).alias("event_actor_count"),
        F.coalesce(F.col("t.event_event_type_count"), F.lit(0)).alias("event_event_type_count"),
    )
)

event_stats_2 = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.count("*").alias("event_count"))
)

profiled = (
    with_counts.alias("s")
    .join(event_stats_2.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

with_windows = (
    profiled
    .select(
        "*",
        F.size(F.collect_set("priority").over(w_prev3_closed_at_to_date_csat)).alias("distinct_priority_in_prev3_closed_at_to_date_csa"),
        F.row_number().over(w_csat_rank_by_csat_rank).alias("csat_rank_seq"),
        F.count(F.lit(1)).over(w_prev3_closed_at_to_date_csat).alias("rows_in_prev3_closed_at_to_date_csat"),
        F.max("csat_rank").over(w_all_by_event_actor_count_pri).alias("csat_rank_max_2_prior"),
        F.round(F.col("requester_id_bucket") / F.nullif(F.sum("requester_id_bucket").over(w_prev3_closed_at_to_date_csat), F.lit(0)), 4).alias("requester_id_bucket_share_of_prev3_closed_at_to_"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        "tags",
        "priority_ref",
        "priority_opened_at_month_sum_csat",
        "distinct_agent_id_in_priority_opened_at_month",
        "prev3_closed_at",
        "rows_in_priority_opened_at_month",
        "event_actor_count",
        "event_event_type_count",
        "event_count",
        "csat_rank_seq",
        "rows_in_prev3_closed_at_to_date_csat",
        "csat_rank_max_2_prior",
        "requester_id_bucket_share_of_prev3_closed_at_to_",
    )
    .orderBy(F.col("id").asc())
)
