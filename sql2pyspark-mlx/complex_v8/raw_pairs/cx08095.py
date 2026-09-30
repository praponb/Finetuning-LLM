from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_ties = Window.orderBy("event_at")
w_all_by_event_at_running = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_by_event_at_full = Window.partitionBy("actor").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_by_event_at = Window.partitionBy("actor").orderBy(F.col("event_at").desc(), "id")
w_all_by_event_at_full = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_by_event_at_ties = Window.partitionBy("actor").orderBy(F.col("event_at").desc())
w_ticket_id_actor_by_event_at = Window.partitionBy("ticket_id", "actor").orderBy(F.col("event_at").desc(), "id")
w_all_by_event_at = Window.orderBy("event_at", "id")
w_second_event_at_month_event = Window.partitionBy(F.month("second_event_at"), "event_at_decile").orderBy("minutes_last_event_at_to_first_known_event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_month = Window.partitionBy(F.month("event_at"))

base = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.substring(F.trim(F.col("event_type")), 1, 3).alias("event_type_prefix"),
        F.when(F.col("event_type").isin("reopened", "resolved"), "group_a").when(F.col("event_type") == "escalated", "group_b").otherwise("other").alias("event_type_group"),
    )
)

metrics = (
    base
    .select(
        "id",
        "event_at",
        "event_type",
        "event_type_prefix",
        "event_type_group",
        F.round(F.cume_dist().over(w_all_by_event_at_ties), 3).alias("event_at_cume_dist"),
        F.first("event_at", ignorenulls=True).over(w_all_by_event_at_running).alias("first_known_event_at"),
        F.nth_value("event_at", 2).over(w_actor_by_event_at_full).alias("second_event_at"),
        F.ntile(10).over(w_actor_by_event_at).alias("event_at_decile"),
        F.last("event_at").over(w_all_by_event_at_full).alias("last_event_at"),
        F.dense_rank().over(w_actor_by_event_at_ties).alias("event_at_dense_rank"),
        F.round(F.cume_dist().over(w_actor_by_event_at), 3).alias("event_at_cume_dist_2"),
        F.round(F.cume_dist().over(w_ticket_id_actor_by_event_at), 3).alias("event_at_cume_dist_3"),
        F.round(F.percent_rank().over(w_all_by_event_at), 3).alias("event_at_percent_rank"),
        F.lag("event_at", 3).over(w_all_by_event_at).alias("prev3_event_at"),
    )
)

annotated = (
    metrics
    .select(
        "id",
        "event_at",
        "event_type_prefix",
        "event_at_cume_dist",
        "first_known_event_at",
        "second_event_at",
        "event_at_decile",
        "event_at_cume_dist_2",
        "event_at_cume_dist_3",
        "event_at_percent_rank",
        F.round((F.unix_timestamp(F.col("first_known_event_at")) - F.unix_timestamp(F.col("last_event_at"))) / 60, 1).alias("minutes_last_event_at_to_first_known_event_at"),
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
    )
)

windowed = (
    annotated
    .select(
        "id",
        "event_type_prefix",
        "event_at_cume_dist",
        "event_at_decile",
        "event_at_cume_dist_2",
        "event_at_cume_dist_3",
        "event_at_percent_rank",
        "minutes_last_event_at_to_first_known_event_at",
        "event_type_or_unknown",
        F.first("event_at").over(w_second_event_at_month_event).alias("first_event_at"),
        F.last("event_at_cume_dist_3").over(w_second_event_at_month_event).alias("last_event_at_cume_dist_3"),
        F.size(F.collect_set("event_type_or_unknown").over(w_event_at_month)).alias("distinct_event_type_or_unknown_in_event_at_month"),
    )
)

result = (
    windowed
    .filter(F.col("event_at_percent_rank") < 0.8)
    .select(
        "id",
        "event_type_prefix",
        "event_at_cume_dist",
        "event_at_decile",
        "event_at_cume_dist_2",
        "event_at_cume_dist_3",
        "event_at_percent_rank",
        "minutes_last_event_at_to_first_known_event_at",
        "event_type_or_unknown",
        "first_event_at",
        "distinct_event_type_or_unknown_in_event_at_month",
        F.to_date(F.col("first_event_at")).alias("first_event_at_date"),
        F.lower(F.regexp_replace(F.trim(F.col("event_type_or_unknown")), " ", "_")).alias("event_type_or_unknown_slug"),
    )
)
