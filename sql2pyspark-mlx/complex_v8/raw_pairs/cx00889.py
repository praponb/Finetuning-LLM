from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at = Window.orderBy("event_at", "id")
w_all_by_event_at_ties = Window.orderBy(F.col("event_at").desc())
w_event_type_actor_by_event_at = Window.partitionBy("event_type", "actor").orderBy(F.col("event_at").desc(), "id")
w_ticket_id_event_type_by_even = Window.partitionBy("ticket_id", "event_type").orderBy("event_at")
w_all_by_event_at_full = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_ticket_events
    .filter((F.col("ticket_id") <= 16) & (F.hour(F.col("event_at")).between(10, 16)))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
        F.when(F.col("event_type").isin("resolved", "escalated"), "group_a").when(F.col("event_type") == "created", "group_b").otherwise("other").alias("event_type_group"),
    )
)

ranked = (
    scoped
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "actor_slug",
        "event_type_group",
        F.ntile(4).over(w_all_by_event_at).alias("event_at_quartile"),
        F.dense_rank().over(w_all_by_event_at_ties).alias("event_at_dense_rank"),
        F.dense_rank().over(w_event_type_actor_by_event_at).alias("event_at_dense_rank_2"),
        F.round(F.cume_dist().over(w_ticket_id_event_type_by_even), 3).alias("event_at_cume_dist"),
        F.last("event_at").over(w_all_by_event_at_full).alias("last_event_at"),
        F.row_number().over(w_event_type_actor_by_event_at).alias("rn"),
    )
)

top_per_group = (
    ranked
    .filter(F.col("event_at_dense_rank") <= 5)
    .drop("event_at_dense_rank")
)

result = (
    top_per_group
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "actor_slug",
        "event_at_quartile",
        "event_at_dense_rank_2",
        "event_at_cume_dist",
        "last_event_at",
        "rn",
    )
    .orderBy("actor_slug", "event_at_dense_rank_2")
)
