from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_by_n_actor_in_sys = Window.partitionBy("event_type").orderBy("n_actor_in_system_customer")
w_event_type_by_n_actor_in_sys_2 = Window.partitionBy("event_type").orderBy("n_actor_in_system_customer", "event_at_month", "event_type").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_n_actor_in_system_customer = Window.partitionBy("n_actor_in_system_customer")
w_event_type_by_n_actor_in_sys_3 = Window.partitionBy("event_type").orderBy("n_actor_in_system_customer", "event_at_month", "event_type")

base_events = (
    df_ticket_events
    .filter(F.month(F.col("event_at")).isin(1, 2))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "yyyy-MM-dd HH:00").alias("event_at_label"),
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
    )
)

stats = (
    base_events
    .groupBy(F.date_trunc("month", "event_at").alias("event_at_month"), "event_type")
    .agg(
        F.array_sort(F.collect_set("actor")).alias("actor_list"),
        F.round(100 * F.count(F.when(F.col("event_type") == "reopened", 1)) / F.count("*"), 1).alias("pct_event_type_reopened"),
        F.count(F.when(F.col("event_type") == "reopened", 1)).alias("n_event_type_reopened"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("event_type") != "resolved", 1).otherwise(0)).alias("n_event_type_resolved"),
        F.count(F.when(F.col("actor").isin("system", "customer"), 1)).alias("n_actor_in_system_customer"),
    )
)

without_matches = (
    stats
    .crossJoin(stats.agg(F.min("distinct_ids").alias("overall_min_distinct_ids")))
    .filter(F.col("distinct_ids") > F.col("overall_min_distinct_ids"))
    .drop("overall_min_distinct_ids")
)

windowed = (
    without_matches
    .select(
        "*",
        F.dense_rank().over(w_event_type_by_n_actor_in_sys).alias("n_actor_in_system_customer_dense_rank"),
        F.nth_value("distinct_ids", 2).over(w_event_type_by_n_actor_in_sys_2).alias("second_distinct_ids"),
        F.round(F.col("distinct_ids") - F.avg("distinct_ids").over(w_n_actor_in_system_customer), 2).alias("distinct_ids_vs_n_actor_in_system_customer_avg"),
        F.row_number().over(w_event_type_by_n_actor_in_sys_3).alias("seq_no"),
        F.round(F.col("n_event_type_resolved") - F.avg("n_event_type_resolved").over(w_n_actor_in_system_customer), 2).alias("n_event_type_resolved_vs_n_actor_in_system_custo"),
    )
)

filtered = (
    windowed
    .crossJoin(windowed.agg(F.max("seq_no").alias("overall_max_seq_no")))
    .filter(F.col("seq_no") < F.col("overall_max_seq_no"))
    .drop("overall_max_seq_no")
)

result = (
    filtered
    .select(
        "event_at_month",
        "event_type",
        "actor_list",
        "pct_event_type_reopened",
        "n_event_type_reopened",
        "distinct_ids",
        "n_actor_in_system_customer",
        "n_actor_in_system_customer_dense_rank",
        "second_distinct_ids",
        "distinct_ids_vs_n_actor_in_system_customer_avg",
        "seq_no",
        "n_event_type_resolved_vs_n_actor_in_system_custo",
        F.date_trunc("month", F.col("event_at_month")).alias("event_at_month_month"),
        F.date_format(F.col("event_at_month"), "yyyy-MM-dd HH:00").alias("event_at_month_label"),
    )
    .orderBy(
        F.col("event_at_month_month").desc_nulls_first(),
        F.col("event_at_month").desc(),
    )
)
