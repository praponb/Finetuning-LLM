from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_ties = Window.orderBy("event_at")
w_all_by_event_at = Window.orderBy("event_at", "id")
w_n_event_type_escalated_by_n = Window.partitionBy("n_event_type_escalated").orderBy("n_event_type_escalated", "event_type", "event_type_group", "event_at_date_ym")
w_n_event_type_escalated_by_n_2 = Window.partitionBy("n_event_type_escalated").orderBy("n_event_type_escalated", "event_type", "event_type_group", "event_at_date_ym").rowsBetween(-6, Window.currentRow)
w_n_event_type_escalated_event = Window.partitionBy("n_event_type_escalated", "event_type")

valid_events = (
    df_ticket_events
    .filter(F.col("event_at") >= F.lit("2025-02-06 10:00:00").cast("timestamp"))
)

metrics = (
    valid_events
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_event_at_ties), 3).alias("event_at_cume_dist"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_all_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
    )
)

enriched = (
    metrics
    .filter(F.col("event_type") == "created")
    .select(
        "*",
        F.to_date(F.col("event_at")).alias("event_at_date"),
        F.when(F.col("event_type").isin("reopened", "resolved"), "group_a").when(F.col("event_type") == "created", "group_b").otherwise("other").alias("event_type_group"),
    )
)

tagged = (
    enriched
    .select(
        "id",
        "event_at",
        "event_type",
        "event_at_cume_dist",
        "minutes_since_prev_event_at",
        "event_at_date",
        "event_type_group",
        F.initcap(F.trim(F.col("event_type_group"))).alias("event_type_group_clean"),
    )
)

totals = (
    tagged
    .filter(F.col("event_type_group").isin("group_a", "group_b"))
    .groupBy(
        "event_type",
        "event_type_group",
        F.date_format("event_at_date", "yyyy-MM").alias("event_at_date_ym"),
    )
    .agg(
        F.min("event_at_cume_dist").alias("min_event_at_cume_dist"),
        F.sum(F.when(F.col("event_type") == "escalated", 1).otherwise(0)).alias("n_event_type_escalated"),
    )
)

with_trends = (
    totals
    .select(
        "*",
        F.lag("n_event_type_escalated", 3).over(w_n_event_type_escalated_by_n).alias("prev3_n_event_type_escalated"),
        F.round(F.avg("n_event_type_escalated").over(w_n_event_type_escalated_by_n_2), 2).alias("n_event_type_escalated_avg_7"),
        F.sum("min_event_at_cume_dist").over(w_n_event_type_escalated_event).alias("n_event_type_escalated_event_type_sum_min_event_"),
    )
)

result = (
    with_trends
    .rollup("prev3_n_event_type_escalated", "event_type_group", "n_event_type_escalated")
    .agg(
        F.grouping("prev3_n_event_type_escalated").alias("is_total_prev3_n_event_type_escalated"),
        F.array_sort(F.collect_set("event_type")).alias("event_type_list"),
        F.count("*").alias("n_rows"),
    )
    .orderBy(
        F.col("prev3_n_event_type_escalated").asc_nulls_last(),
        F.col("event_type_group").asc_nulls_last(),
        F.col("n_event_type_escalated").asc_nulls_last(),
    )
)
