from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_closed_at = Window.orderBy("closed_at", "id")
w_all_by_n_event_type_in_reope = Window.orderBy("n_event_type_in_reopened_creat", "event_id", "event_type", "event_actor").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_n_event_type_in_reope_2 = Window.orderBy("n_event_type_in_reopened_creat", "event_id", "event_type", "event_actor")
w_all_by_n_event_type_in_reope_3 = Window.orderBy("n_event_type_in_reopened_creat")
w_all_by_n_event_type_in_reope_4 = Window.orderBy("n_event_type_in_reopened_creat", "event_id", "event_type", "event_actor").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = (
    df_tickets
    .filter(F.col("opened_at") < F.lit("2025-02-18 09:00:00").cast("timestamp"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
    )
)

ranked = (
    scoped
    .select(
        "id",
        "priority",
        F.rank().over(w_all_by_closed_at).alias("closed_at_rank"),
    )
)

with_children = (
    ranked.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        F.col("t.id").alias("event_id"),
        "t.event_at",
        "t.event_type",
        F.col("t.actor").alias("event_actor"),
    )
)

grouped = (
    with_children
    .groupBy("event_id", "event_type", "event_actor")
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("event_id").alias("distinct_event_ids"),
        F.round(100 * F.count(F.when(F.col("event_actor").isNotNull(), 1)) / F.count("*"), 1).alias("pct_event_actor_is_not_null"),
        F.sum(F.when(F.col("event_type").isin("reopened", "created", "assigned"), 1).otherwise(0)).alias("n_event_type_in_reopened_creat"),
        F.array_sort(F.collect_set("event_actor")).alias("event_actor_list"),
    )
)

tagged = (
    grouped
    .filter(F.col("pct_event_actor_is_not_null") < 60.0)
    .withColumn("distinct_event_ids_bucket", F.floor(F.col("distinct_event_ids") / 2) * 2)
)

windowed = (
    tagged
    .select(
        "*",
        F.nth_value("pct_event_actor_is_not_null", 2).over(w_all_by_n_event_type_in_reope).alias("second_pct_event_actor_is_not_null"),
        F.lead("distinct_event_ids", 1, 0).over(w_all_by_n_event_type_in_reope_2).alias("next_distinct_event_ids"),
        (F.col("distinct_event_ids_bucket") - F.lag("distinct_event_ids_bucket").over(w_all_by_n_event_type_in_reope_2)).alias("distinct_event_ids_bucket_change"),
        F.round(F.percent_rank().over(w_all_by_n_event_type_in_reope_3), 3).alias("n_event_type_in_reopened_creat_percent_rank"),
        F.count(F.lit(1)).over(w_all_by_n_event_type_in_reope_4).alias("running_count"),
    )
)

result = (
    windowed
    .select(
        "event_id",
        "event_type",
        "event_actor",
        "distinct_event_ids",
        "n_event_type_in_reopened_creat",
        "event_actor_list",
        "distinct_event_ids_bucket",
        "second_pct_event_actor_is_not_null",
        "next_distinct_event_ids",
        "distinct_event_ids_bucket_change",
        "n_event_type_in_reopened_creat_percent_rank",
        "running_count",
        F.substring(F.trim(F.col("event_type")), 1, 3).alias("event_type_prefix"),
        F.substring(F.trim(F.col("event_actor")), 1, 2).alias("event_actor_prefix"),
    )
    .orderBy(F.col("distinct_event_ids_bucket").desc())
)
