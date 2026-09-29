from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_event_type_by_event_at = Window.partitionBy("actor", "event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_plus_7d_full = Window.orderBy("event_at_plus_7d", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_plus_7d = Window.orderBy("event_at_plus_7d", "id")
w_ticket_id_actor_by_event_at = Window.partitionBy("ticket_id", "actor").orderBy("event_at")
w_actor_event_type_by_event_at_2 = Window.partitionBy("actor", "event_type").orderBy("event_at")
w_actor_event_type_by_event_at_3 = Window.partitionBy("actor", "event_type").orderBy("event_at", "id")
w_ticket_id_actor_by_event_at_2 = Window.partitionBy("ticket_id", "actor").orderBy("event_at", "id")

base_events = (
    df_ticket_events
    .filter(F.col("event_type").isin("escalated", "created", "assigned"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        (F.col("event_at") + F.expr("INTERVAL 7 DAYS")).alias("event_at_plus_7d"),
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
    )
)

metrics = (
    base_events
    .select(
        "*",
        F.last("event_at_plus_7d").over(w_actor_event_type_by_event_at).alias("last_event_at_plus_7d"),
        F.nth_value("event_at_plus_7d", 2).over(w_all_by_event_at_plus_7d_full).alias("second_event_at_plus_7d"),
        F.ntile(5).over(w_all_by_event_at_plus_7d).alias("event_at_plus_7d_quintile"),
        F.round(F.cume_dist().over(w_ticket_id_actor_by_event_at), 3).alias("event_at_cume_dist"),
        F.lag("event_at").over(w_all_by_event_at_plus_7d).alias("prev_event_at"),
        F.round(F.cume_dist().over(w_actor_event_type_by_event_at_2), 3).alias("event_at_cume_dist_2"),
        F.round(F.percent_rank().over(w_actor_event_type_by_event_at_3), 3).alias("event_at_percent_rank"),
        F.last("event_at").over(w_all_by_event_at_plus_7d_full).alias("last_event_at"),
        F.lag("event_at").over(w_ticket_id_actor_by_event_at_2).alias("prev_event_at_2"),
        F.rank().over(w_ticket_id_actor_by_event_at_2).alias("event_at_rank"),
    )
)

result = (
    metrics
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "actor_or_unknown",
        "second_event_at_plus_7d",
        "event_at_cume_dist_2",
        "event_at_percent_rank",
        "last_event_at",
        "prev_event_at_2",
        "event_at_rank",
    )
    .orderBy(F.col("event_at_rank").desc(), "id")
    .limit(5)
)
