from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("event_type_or_unknown").orderBy("first_event_at", "actor", "ticket_id", "event_type")

valid = (
    df_ticket_events
    .filter(F.col("ticket_id") >= 8)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("event_type").isin("replied", "created"), "group_a").when(F.col("event_type") == "resolved", "group_b").otherwise("other").alias("event_type_group"),
    )
)

grouped = (
    valid
    .groupBy("actor", "ticket_id", "event_type")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.min("event_at").alias("first_event_at"),
        F.count(F.when(F.col("event_type").isin("reopened", "created", "resolved"), 1)).alias("n_event_type_in_reopened_creat"),
        F.count("*").alias("row_count"),
        F.countDistinct("ticket_id").alias("distinct_ticket_ids"),
        F.array_sort(F.collect_set("event_type")).alias("event_type_list"),
    )
)

labeled = (
    grouped
    .select(
        "*",
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
        F.abs(F.col("distinct_ticket_ids") - 4).alias("distinct_ticket_ids_distance"),
        F.concat(F.col("actor"), F.lit("#"), F.col("ticket_id").cast("string")).alias("actor_ref"),
    )
)

deduped = (
    labeled
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

with_activity = (
    deduped.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type").isin("escalated", "resolved", "created")) & (F.col("t.event_at") >= F.lit("2025-01-25 07:00:00").cast("timestamp")), "left_semi")
    .filter(F.col("actor") == "agent")
)

result = (
    with_activity
    .select(
        "actor",
        "event_type",
        "distinct_ids",
        "first_event_at",
        "n_event_type_in_reopened_creat",
        "row_count",
        "distinct_ticket_ids",
        "event_type_or_unknown",
        F.coalesce(F.col("row_count"), F.lit(0)).alias("row_count_filled"),
    )
    .orderBy(F.col("n_event_type_in_reopened_creat").desc_nulls_first())
)
