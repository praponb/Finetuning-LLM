from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_event_type_resolved_by_dis = Window.partitionBy("n_event_type_resolved").orderBy("distinct_ids", "event_at_plus_15m_month").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_ids_by_distinct_ids = Window.partitionBy("distinct_ids").orderBy("distinct_ids", "event_at_plus_15m_month")
w_distinct_ids_by_pct_event_ty = Window.partitionBy("distinct_ids").orderBy("pct_event_type_not_in_assigned_r")
w_distinct_ids_by_distinct_ids_2 = Window.partitionBy("distinct_ids").orderBy("distinct_ids")

valid_events = (
    df_ticket_events
    .filter((F.col("ticket_id") >= 20) | (F.col("actor").isin("system", "customer")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        (F.col("event_at") + F.expr("INTERVAL 15 MINUTES")).alias("event_at_plus_15m"),
    )
)

grouped = (
    valid_events
    .groupBy(
        F.date_trunc("month", "event_at_plus_15m").alias("event_at_plus_15m_month"),
    )
    .agg(
        F.sum(F.when(F.col("event_type") == "resolved", 1).otherwise(0)).alias("n_event_type_resolved"),
        F.round(100 * F.count(F.when(~F.col("event_type").isin("assigned", "reopened"), 1)) / F.count("*"), 1).alias("pct_event_type_not_in_assigned_r"),
        F.array_sort(F.collect_set("actor")).alias("actor_list"),
        F.countDistinct("id").alias("distinct_ids"),
        F.array_sort(F.collect_set("event_type")).alias("event_type_list"),
        F.round(100 * F.count(F.when(F.col("actor") == "system", 1)) / F.count("*"), 1).alias("pct_actor_system"),
    )
)

screened = (
    grouped
    .crossJoin(grouped.agg(F.avg("n_event_type_resolved").alias("overall_avg_n_event_type_resolved")))
    .filter(F.col("n_event_type_resolved") > F.col("overall_avg_n_event_type_resolved"))
    .drop("overall_avg_n_event_type_resolved")
)

without_matches = (
    screened
    .crossJoin(screened.agg(F.max("distinct_ids").alias("overall_max_distinct_ids")))
    .filter(F.col("distinct_ids") < F.col("overall_max_distinct_ids"))
    .drop("overall_max_distinct_ids")
)

with_trends = (
    without_matches
    .select(
        "*",
        F.nth_value("n_event_type_resolved", 2).over(w_n_event_type_resolved_by_dis).alias("second_n_event_type_resolved"),
        F.lag("event_at_plus_15m_month", 3).over(w_distinct_ids_by_distinct_ids).alias("prev3_event_at_plus_15m_month"),
        F.last("distinct_ids").over(w_n_event_type_resolved_by_dis).alias("last_distinct_ids"),
        F.rank().over(w_distinct_ids_by_pct_event_ty).alias("pct_event_type_not_in_assigned_r_rank"),
        F.dense_rank().over(w_distinct_ids_by_distinct_ids_2).alias("distinct_ids_dense_rank"),
        F.ntile(3).over(w_distinct_ids_by_distinct_ids).alias("distinct_ids_tercile"),
    )
)

exploded = (
    with_trends
    .select(
        "event_at_plus_15m_month",
        "n_event_type_resolved",
        "pct_event_type_not_in_assigned_r",
        "actor_list",
        "distinct_ids",
        "pct_actor_system",
        "second_n_event_type_resolved",
        "prev3_event_at_plus_15m_month",
        "last_distinct_ids",
        "pct_event_type_not_in_assigned_r_rank",
        "distinct_ids_dense_rank",
        "distinct_ids_tercile",
        F.explode_outer("event_type_list").alias("event_type_list_item"),
    )
)

enriched = (
    exploded
    .filter(F.month(F.col("prev3_event_at_plus_15m_month")).isin(1, 3))
    .select(
        "*",
        F.round(F.col("pct_actor_system") * 1.5, 2).alias("pct_actor_system_adj"),
        F.concat(F.col("event_type_list_item"), F.lit("#"), F.col("distinct_ids_tercile").cast("string")).alias("event_type_list_item_ref"),
        F.coalesce(F.col("n_event_type_resolved"), F.lit(0)).alias("n_event_type_resolved_filled"),
    )
)

result = (
    enriched
    .select(
        "event_at_plus_15m_month",
        "n_event_type_resolved",
        "pct_event_type_not_in_assigned_r",
        "actor_list",
        "distinct_ids",
        "pct_actor_system",
        "second_n_event_type_resolved",
        "prev3_event_at_plus_15m_month",
        "pct_event_type_not_in_assigned_r_rank",
        "distinct_ids_tercile",
        "event_type_list_item",
        "pct_actor_system_adj",
        "event_type_list_item_ref",
        "n_event_type_resolved_filled",
        F.abs(F.col("distinct_ids_dense_rank") - 7).alias("distinct_ids_dense_rank_distance"),
    )
    .orderBy(F.col("n_event_type_resolved").desc_nulls_first())
)
