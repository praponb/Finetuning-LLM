from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_row_count = Window.partitionBy("actor", "row_count")
w_actor_or_unknown_actor_by_ac = Window.partitionBy("actor_or_unknown", "actor").orderBy(F.col("actor_row_count_max_row_count").desc(), "actor_or_unknown", "actor", "event_at_month")
w_distinct_actor_or_unknown_in = Window.partitionBy("distinct_actor_or_unknown_in_actor_row_count", "actor_or_unknown").orderBy("row_count", "actor_or_unknown", "actor", "event_at_month")

filtered_events = (
    df_ticket_events
    .filter(F.col("event_type") == "escalated")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "yyyy-MM-dd HH:00").alias("event_at_label"),
        F.date_format(F.col("event_at"), "yyyy-MM-dd HH:00").alias("event_at_label_2"),
        F.concat_ws("|", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
    )
)

tagged = (
    filtered_events
    .filter(F.col("event_at") < F.lit("2025-02-12 12:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "event_type_actor_label",
        F.concat(F.col("actor"), F.lit("#"), F.col("id").cast("string")).alias("actor_ref"),
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
        F.date_format(F.col("event_at"), "yyyy-MM-dd").alias("event_at_label_3"),
    )
)

stats = (
    tagged
    .groupBy(
        "actor_or_unknown",
        "actor",
        F.date_trunc("month", "event_at").alias("event_at_month"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.array_sort(F.collect_set("actor")).alias("actor_list"),
        F.round(100 * F.count(F.when(F.col("actor").isin("system", "customer"), 1)) / F.count("*"), 1).alias("pct_actor_in_system_customer"),
    )
)

filtered = (
    stats
    .crossJoin(stats.agg(F.avg("row_count").alias("overall_avg_row_count")))
    .filter(F.col("row_count") > F.col("overall_avg_row_count"))
    .drop("overall_avg_row_count")
)

windowed = (
    filtered
    .select(
        "*",
        F.max("row_count").over(w_actor_row_count).alias("actor_row_count_max_row_count"),
        F.size(F.collect_set("actor_or_unknown").over(w_actor_row_count)).alias("distinct_actor_or_unknown_in_actor_row_count"),
        F.sum("pct_actor_in_system_customer").over(w_actor_row_count).alias("actor_row_count_sum_pct_actor_in_system_customer"),
    )
)

labeled = (
    windowed
    .select(
        "actor_or_unknown",
        "actor",
        "event_at_month",
        "row_count",
        "actor_row_count_max_row_count",
        "distinct_actor_or_unknown_in_actor_row_count",
        "actor_row_count_sum_pct_actor_in_system_customer",
        F.size(F.col("actor_list")).alias("actor_list_count"),
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown_2"),
    )
)

metrics = (
    labeled
    .select(
        "*",
        F.lag("actor_list_count", 2).over(w_actor_or_unknown_actor_by_ac).alias("prev2_actor_list_count"),
        F.row_number().over(w_distinct_actor_or_unknown_in).alias("row_count_seq"),
    )
)

result = (
    metrics
    .select(
        "actor_or_unknown",
        "actor",
        "event_at_month",
        "distinct_actor_or_unknown_in_actor_row_count",
        "actor_row_count_sum_pct_actor_in_system_customer",
        "actor_or_unknown_2",
        "prev2_actor_list_count",
        "row_count_seq",
        (F.col("actor_list_count") + F.col("prev2_actor_list_count")).alias("actor_list_count_plus_prev2_actor_list_count"),
        F.date_format(F.col("event_at_month"), "yyyy-MM-dd HH:00").alias("event_at_month_label"),
    )
)
