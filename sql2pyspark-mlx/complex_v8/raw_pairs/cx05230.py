from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_ticket_id_by_event_at = Window.partitionBy("actor", "ticket_id").orderBy("event_at", "id")
w_event_type_ticket_id_by_even = Window.partitionBy("event_type", "ticket_id").orderBy(F.col("event_at").desc_nulls_first(), "id")
w_all_by_event_at = Window.orderBy(F.col("event_at").desc(), "id")
w_event_type_actor_by_event_at = Window.partitionBy("event_type", "actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_ties = Window.orderBy(F.col("event_at").desc())
w_all_by_event_at_full = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_actor_by_event_at_2 = Window.partitionBy("event_type", "actor").orderBy("event_at")
w_event_type_actor_by_event_at_3 = Window.partitionBy("event_type", "actor").orderBy("event_at", "id")

recent = df_ticket_events.filter(F.col("event_type") == "escalated")

sequenced = (
    recent
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        F.lag("event_at", 2).over(w_actor_ticket_id_by_event_at).alias("prev2_event_at"),
        F.lag("event_at").over(w_event_type_ticket_id_by_even).alias("prev_event_at"),
        F.lead("event_at", 3).over(w_all_by_event_at).alias("next3_event_at"),
        F.nth_value("event_at", 2).over(w_event_type_actor_by_event_at).alias("second_event_at"),
        F.round(F.cume_dist().over(w_all_by_event_at_ties), 3).alias("event_at_cume_dist"),
        F.nth_value("event_at", 2).over(w_all_by_event_at_full).alias("second_event_at_2"),
        F.round(F.percent_rank().over(w_event_type_actor_by_event_at_2), 3).alias("event_at_percent_rank"),
        F.round(F.cume_dist().over(w_event_type_actor_by_event_at_3), 3).alias("event_at_cume_dist_2"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "prev2_event_at",
        "prev_event_at",
        "next3_event_at",
        "second_event_at",
        "second_event_at_2",
        "event_at_percent_rank",
        "event_at_cume_dist_2",
        (F.col("event_at") + F.expr("INTERVAL 15 MINUTES")).alias("event_at_plus_15m"),
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
    )
    .orderBy(
        F.col("prev2_event_at").desc_nulls_first(),
        F.col("second_event_at").desc(),
    )
)
