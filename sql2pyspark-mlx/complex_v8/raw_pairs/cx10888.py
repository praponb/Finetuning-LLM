from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_event_type_len = Window.partitionBy("event_type", "event_type_len")
w_all_by_event_at = Window.orderBy("event_at", "id")
w_all_by_event_at_full = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_event_type_by_even = Window.partitionBy("ticket_id", "event_type").orderBy(F.col("event_at").desc(), "id")
w_dedup = Window.partitionBy("actor").orderBy("event_at_decile", "id")

scoped_events = (
    df_ticket_events
    .filter(F.col("actor") != "agent")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.trim(F.col("actor"))).alias("actor_clean"),
        F.upper(F.trim(F.col("event_type"))).alias("event_type_clean"),
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
    )
)

with_windows = (
    scoped_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        F.sum("event_type_len").over(w_event_type_event_type_len).alias("event_type_event_type_len_sum_event_type_len"),
        F.ntile(10).over(w_all_by_event_at).alias("event_at_decile"),
        F.last("event_type_len").over(w_all_by_event_at_full).alias("last_event_type_len"),
        F.ntile(5).over(w_ticket_id_event_type_by_even).alias("event_at_quintile"),
    )
)

latest_per_key = (
    with_windows
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

rollup_base = (
    latest_per_key
    .filter(F.to_date(F.col("event_at")) < F.lit("2025-02-06").cast("date"))
    .groupBy("event_at_decile", "event_at_quintile")
    .agg(
        F.sum(F.when(F.col("actor") == "system", 1).otherwise(0)).alias("n_actor_system"),
        F.round(F.stddev("last_event_type_len"), 2).alias("stddev_last_event_type_len"),
        F.count("*").alias("row_count"),
        F.countDistinct("actor").alias("distinct_actors"),
        F.array_sort(F.collect_set("actor")).alias("actor_list"),
        F.percentile_approx("event_at_decile", 0.5).alias("median_event_at_decile"),
    )
    .filter(F.col("row_count") >= 2)
)

labeled = (
    rollup_base
    .select(
        "event_at_decile",
        "event_at_quintile",
        "n_actor_system",
        "row_count",
        "median_event_at_decile",
        F.when(F.col("n_actor_system") >= 8, "large").when(F.col("n_actor_system") >= 2, "medium").otherwise("small").alias("n_actor_system_tier"),
    )
)

result = (
    labeled
    .select(
        "event_at_decile",
        "event_at_quintile",
        "median_event_at_decile",
        "n_actor_system_tier",
        F.least(F.col("row_count"), F.col("n_actor_system")).alias("least_row_count_n_actor_system"),
    )
    .orderBy(F.col("median_event_at_decile").asc_nulls_last())
)
