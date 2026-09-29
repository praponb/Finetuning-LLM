from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_by_event_at = Window.partitionBy("event_type").orderBy("event_at", "id")
w_actor_event_type_by_event_at = Window.partitionBy("actor", "event_type").orderBy("event_at", "id")
w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_actor_event_type_by_event_at_2 = Window.partitionBy("actor", "event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent_events = (
    df_ticket_events
    .filter(F.month(F.col("event_at")).isin(1, 2))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

metrics = (
    recent_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.row_number().over(w_event_type_by_event_at).alias("seq_no"),
        F.round(F.cume_dist().over(w_actor_event_type_by_event_at), 3).alias("event_at_cume_dist"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_ticket_id_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.lag("event_at").over(w_actor_event_type_by_event_at).alias("prev_event_at"),
        F.ntile(4).over(w_actor_event_type_by_event_at).alias("event_at_quartile"),
        F.last("event_at").over(w_actor_event_type_by_event_at_2).alias("last_event_at"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_actor_event_type_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at_2"),
    )
)

top_per_group = (
    metrics
    .filter(F.col("seq_no") == 1)
    .drop("seq_no")
)

result = (
    top_per_group
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "minutes_since_prev_event_at",
        "prev_event_at",
        "last_event_at",
        "minutes_since_prev_event_at_2",
    )
    .orderBy(F.col("ticket_id").desc())
)
