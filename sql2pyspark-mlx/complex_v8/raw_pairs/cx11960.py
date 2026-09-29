from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_or_unknown = Window.partitionBy("actor_or_unknown")
w_all_by_event_at = Window.orderBy("event_at", "id")

base = (
    df_ticket_events
    .filter(F.col("ticket_id") <= 36)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
        F.when(F.col("event_type").isin("assigned", "reopened"), "group_a").when(F.col("event_type") == "escalated", "group_b").otherwise("other").alias("event_type_group"),
    )
)

with_parent_ticket = (
    base.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.actor_or_unknown",
        "s.event_type_group",
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.requester_id").alias("ticket_requester_id"),
    )
)

with_windows = (
    with_parent_ticket
    .withColumn("rows_in_actor_or_unknown", F.count(F.lit(1)).over(w_actor_or_unknown))
    .withColumn("event_at_dense_rank", F.dense_rank().over(w_all_by_event_at))
)

result = (
    with_windows
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "actor_or_unknown",
        "ticket_tags",
        "ticket_requester_id",
        "rows_in_actor_or_unknown",
        "event_at_dense_rank",
        F.abs(F.col("ticket_requester_id") - 17).alias("ticket_requester_id_distance"),
    )
    .orderBy(
        F.col("event_at_dense_rank").desc_nulls_first(),
        F.col("rows_in_actor_or_unknown").desc(),
    )
)
