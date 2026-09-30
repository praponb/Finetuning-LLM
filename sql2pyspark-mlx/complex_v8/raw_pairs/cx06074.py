from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_distinct_actors_event_type_l = Window.partitionBy("distinct_actors", "event_type_len")
w_all_by_n_rows_full = Window.orderBy("n_rows", "event_at_month", "event_type_len", "event_at_plus_30m_day").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        (F.col("event_at") + F.expr("INTERVAL 30 MINUTES")).alias("event_at_plus_30m"),
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
    )
)

rollup_base = (
    recent_events
    .groupBy(
        F.date_trunc("month", "event_at").alias("event_at_month"),
        "event_type_len",
        F.to_date("event_at_plus_30m").alias("event_at_plus_30m_day"),
    )
    .agg(
        F.round(F.stddev("event_type_len"), 2).alias("stddev_event_type_len"),
        F.count("*").alias("n_rows"),
        F.countDistinct("actor").alias("distinct_actors"),
        F.max("event_type_len").alias("max_event_type_len"),
        F.round(100 * F.count(F.when(~F.col("event_type").isin("escalated", "assigned"), 1)) / F.count("*"), 1).alias("pct_event_type_not_in_escalated"),
        F.countDistinct("event_type").alias("distinct_event_types"),
    )
    .filter(F.col("n_rows") >= 2)
)

metrics = (
    rollup_base
    .select(
        "event_at_month",
        "event_type_len",
        "event_at_plus_30m_day",
        "n_rows",
        "pct_event_type_not_in_escalated",
        F.round(F.col("max_event_type_len") / F.nullif(F.sum("max_event_type_len").over(w_distinct_actors_event_type_l), F.lit(0)), 4).alias("max_event_type_len_share_of_distinct_actors_even"),
        F.last("event_at_month").over(w_all_by_n_rows_full).alias("last_event_at_month"),
        F.nth_value("event_type_len", 2).over(w_all_by_n_rows_full).alias("second_event_type_len"),
        F.round(F.col("distinct_event_types") / F.nullif(F.sum("distinct_event_types").over(w_distinct_actors_event_type_l), F.lit(0)), 4).alias("distinct_event_types_share_of_distinct_actors_ev"),
    )
)

without_matches = (
    metrics
    .crossJoin(metrics.agg(F.avg("max_event_type_len_share_of_distinct_actors_even").alias("overall_avg_max_event_type_len_share_of_distinct")))
    .filter(F.col("max_event_type_len_share_of_distinct_actors_even") > F.col("overall_avg_max_event_type_len_share_of_distinct"))
    .drop("overall_avg_max_event_type_len_share_of_distinct")
)

with_activity = (
    without_matches
    .crossJoin(without_matches.agg(F.avg("event_type_len").alias("overall_avg_event_type_len")))
    .filter(F.col("event_type_len") > F.col("overall_avg_event_type_len"))
    .drop("overall_avg_event_type_len")
)

screened = (
    with_activity
    .crossJoin(with_activity.agg(F.min("n_rows").alias("overall_min_n_rows")))
    .filter(F.col("n_rows") > F.col("overall_min_n_rows"))
    .drop("overall_min_n_rows")
)

result = (
    screened
    .select(
        "event_at_month",
        "event_at_plus_30m_day",
        "n_rows",
        "pct_event_type_not_in_escalated",
        "max_event_type_len_share_of_distinct_actors_even",
        "last_event_at_month",
        "second_event_type_len",
        "distinct_event_types_share_of_distinct_actors_ev",
    )
    .orderBy(
        F.col("last_event_at_month").asc(),
        "distinct_event_types_share_of_distinct_actors_ev",
    )
)
