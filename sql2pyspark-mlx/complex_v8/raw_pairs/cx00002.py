from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_priority_event_type_b = Window.partitionBy("ticket_priority", "event_type").orderBy("days_since_event_at_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_priority_event_type_b_2 = Window.partitionBy("ticket_priority", "event_type").orderBy("days_since_event_at_2", "id")
w_ticket_priority_event_type = Window.partitionBy("ticket_priority", "event_type")
w_ticket_priority_event_type_b_3 = Window.partitionBy("ticket_priority", "event_type").orderBy("days_since_event_at_2", "id").rowsBetween(-6, -1)

valid = (
    df_ticket_events
    .filter((F.col("event_type") == "reopened") & (F.col("ticket_id").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")).alias("days_since_event_at_2"),
        F.concat_ws("-", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
    )
)

with_details = (
    valid.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.*",
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.requester_id").alias("ticket_requester_id"),
    )
)

metrics = (
    with_details
    .select(
        "id",
        "event_at",
        "event_type",
        "days_since_event_at",
        "event_type_actor_label",
        "ticket_priority",
        "ticket_tags",
        F.sum("days_since_event_at").over(w_ticket_priority_event_type_b).alias("running_days_since_event_at"),
        F.rank().over(w_ticket_priority_event_type_b_2).alias("days_since_event_at_2_rank"),
        F.min("ticket_requester_id").over(w_ticket_priority_event_type).alias("ticket_priority_event_type_min_ticket_requester_"),
        F.sum("ticket_requester_id").over(w_ticket_priority_event_type_b_3).alias("ticket_requester_id_sum_6_prior"),
        F.size(F.collect_set("ticket_id").over(w_ticket_priority_event_type)).alias("distinct_ticket_id_in_ticket_priority_event_type"),
    )
)

result = (
    metrics
    .groupingSets(
        [["distinct_ticket_id_in_ticket_priority_event_type", "event_type"], ["distinct_ticket_id_in_ticket_priority_event_type"]],
        "distinct_ticket_id_in_ticket_priority_event_type", "event_type",
    )
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.sum(F.when(F.col("ticket_priority") == "P4", 1).otherwise(0)).alias("n_ticket_priority_p4"),
        F.max("event_at").alias("last_event_at"),
    )
    .orderBy(
        F.col("distinct_ticket_id_in_ticket_priority_event_type").asc_nulls_last(),
        F.col("event_type").asc_nulls_last(),
    )
)
