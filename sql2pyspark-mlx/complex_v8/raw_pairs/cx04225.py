from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_by_row_count = Window.partitionBy("actor").orderBy("row_count", "actor", "event_type_or_unknown")
w_n_event_type_not_in_reopened = Window.partitionBy("n_event_type_not_in_reopened_c")
w_all_by_n_event_type_is_not_n = Window.orderBy("n_event_type_is_not_null", "actor", "event_type_or_unknown").rowsBetween(-3, -1)

filtered_events = (
    df_ticket_events
    .filter((F.col("event_type") == "assigned") & (F.month(F.col("event_at")).isin(1, 2)))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.substring(F.trim(F.col("event_type")), 1, 3).alias("event_type_prefix"),
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
        F.date_trunc("week", F.col("event_at")).alias("event_at_week"),
    )
)

grouped = (
    filtered_events
    .groupBy("actor", "event_type_or_unknown")
    .agg(
        F.count(F.when(~F.col("event_type").isin("reopened", "created"), 1)).alias("n_event_type_not_in_reopened_c"),
        F.countDistinct("event_type").alias("distinct_event_types"),
        F.round(100 * F.count(F.when(F.col("event_type") == "reopened", 1)) / F.count("*"), 1).alias("pct_event_type_reopened"),
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("event_type").isNotNull(), 1)).alias("n_event_type_is_not_null"),
    )
)

metrics = (
    grouped
    .select(
        "*",
        F.lead("distinct_event_types").over(w_actor_by_row_count).alias("next_distinct_event_types"),
        F.size(F.collect_set("event_type_or_unknown").over(w_n_event_type_not_in_reopened)).alias("distinct_event_type_or_unknown_in_n_event_type_n"),
        F.round(F.col("distinct_event_types") / F.nullif(F.sum("distinct_event_types").over(w_n_event_type_not_in_reopened), F.lit(0)), 4).alias("distinct_event_types_share_of_n_event_type_not_i"),
        F.sum("n_event_type_is_not_null").over(w_all_by_n_event_type_is_not_n).alias("n_event_type_is_not_null_sum_3_prior"),
    )
)

result = (
    metrics
    .select(
        "actor",
        "event_type_or_unknown",
        "n_event_type_not_in_reopened_c",
        "distinct_event_types",
        "pct_event_type_reopened",
        "row_count",
        "n_event_type_is_not_null",
        "next_distinct_event_types",
        "distinct_event_type_or_unknown_in_n_event_type_n",
        "n_event_type_is_not_null_sum_3_prior",
        F.initcap(F.trim(F.col("actor"))).alias("actor_clean"),
    )
    .orderBy(
        F.col("actor").desc(),
        F.col("distinct_event_types").desc(),
        "event_type_or_unknown",
    )
    .limit(20)
)
