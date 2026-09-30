from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("actor", "ticket_id").orderBy("event_at", "id")
w_event_type_ticket_id = Window.partitionBy("event_type", "ticket_id")
w_all_by_event_at_full = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent = (
    df_ticket_events
    .filter(F.col("ticket_id") <= 6)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "yyyy-MM-dd HH:00").alias("event_at_label"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.concat_ws(": ", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
    )
)

unique_rows = (
    recent
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

metrics = (
    unique_rows
    .select(
        "*",
        F.size(F.collect_set("actor").over(w_event_type_ticket_id)).alias("distinct_actor_in_event_type_ticket_id"),
        F.min("days_since_event_at").over(w_event_type_ticket_id).alias("event_type_ticket_id_min_days_since_event_at"),
        F.first("event_at").over(w_all_by_event_at_full).alias("first_event_at"),
        F.round(F.col("days_since_event_at") / F.nullif(F.sum("days_since_event_at").over(w_event_type_ticket_id), F.lit(0)), 4).alias("days_since_event_at_share_of_event_type_ticket_i"),
    )
)

result = (
    metrics
    .groupingSets(
        [["event_type", "distinct_actor_in_event_type_ticket_id"], ["event_type"], ["distinct_actor_in_event_type_ticket_id"], []],
        "event_type", "distinct_actor_in_event_type_ticket_id",
    )
    .agg(
        F.round(F.sum("days_since_event_at") / F.nullif(F.sum("distinct_actor_in_event_type_ticket_id"), F.lit(0)), 3).alias("days_since_event_at_to_distinct_actor_in_event_t"),
    )
    .orderBy(
        F.col("event_type").asc_nulls_last(),
        F.col("distinct_actor_in_event_type_ticket_id").asc_nulls_last(),
    )
)
