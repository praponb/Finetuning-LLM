from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_at_month_by_event_at = Window.partitionBy(F.month("event_at")).orderBy(F.col("event_at").desc(), "id")
w_actor_by_event_at = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_ticket_id_by_event_at = Window.partitionBy("actor", "ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_ticket_id_by_event_at_2 = Window.partitionBy("actor", "ticket_id").orderBy("event_at")
w_actor_by_event_at_ties = Window.partitionBy("actor").orderBy("event_at")
w_actor_ticket_id_by_event_at_3 = Window.partitionBy("actor", "ticket_id").orderBy("event_at", "id")

base_events = (
    df_ticket_events
    .filter((F.hour(F.col("event_at")).between(9, 13)) & (F.col("actor").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

sequenced = (
    base_events
    .select(
        "*",
        F.round(F.cume_dist().over(w_event_at_month_by_event_at), 3).alias("event_at_cume_dist"),
        F.lead("event_at").over(w_actor_by_event_at).alias("next_event_at"),
        F.last("event_at", ignorenulls=True).over(w_actor_ticket_id_by_event_at).alias("last_known_event_at"),
        F.lag("event_at", 2).over(w_actor_by_event_at).alias("prev2_event_at"),
        F.rank().over(w_actor_ticket_id_by_event_at_2).alias("event_at_rank"),
        F.round(F.percent_rank().over(w_event_at_month_by_event_at), 3).alias("event_at_percent_rank"),
        F.round(F.cume_dist().over(w_actor_by_event_at_ties), 3).alias("event_at_cume_dist_3"),
        F.lag("event_at").over(w_actor_ticket_id_by_event_at_3).alias("prev_event_at"),
    )
)

result = (
    sequenced
    .filter(F.col("ticket_id") <= 25)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "last_known_event_at",
        "event_at_rank",
        "event_at_percent_rank",
        "event_at_cume_dist_3",
        F.abs(F.col("event_at_rank") - 7).alias("event_at_rank_distance"),
    )
    .orderBy(F.col("ticket_id").desc_nulls_first(), "event_at_rank_distance")
)
