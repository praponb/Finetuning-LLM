from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at = Window.orderBy(F.col("event_at").desc(), "id")
w_event_type_by_event_at = Window.partitionBy("event_type").orderBy(F.col("event_at").desc(), "id")
w_all_by_event_at_ties = Window.orderBy(F.col("event_at").desc())
w_actor_by_event_at_moving_2 = Window.partitionBy("actor").orderBy(F.col("event_at").desc(), "id").rowsBetween(-2, Window.currentRow)
w_actor_by_event_at = Window.partitionBy("actor").orderBy(F.col("event_at").desc(), "id")

scoped = df_ticket_events.filter(F.col("ticket_id") >= 33)

with_windows = (
    scoped
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        F.rank().over(w_all_by_event_at).alias("event_at_rank"),
        F.lead("event_at").over(w_event_type_by_event_at).alias("next_event_at"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_event_type_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.round(F.percent_rank().over(w_all_by_event_at_ties), 3).alias("event_at_percent_rank"),
    )
)

top_per_group = with_windows.filter(F.col("event_at_rank") <= 3)

sequenced = (
    top_per_group
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_at_rank",
        F.round(F.avg("minutes_since_prev_event_at").over(w_actor_by_event_at_moving_2), 2).alias("minutes_since_prev_event_at_avg_3"),
        F.ntile(3).over(w_actor_by_event_at).alias("event_at_tercile"),
        F.lag("next_event_at").over(w_actor_by_event_at).alias("prev_next_event_at"),
    )
)

result = (
    sequenced
    .select("id", "ticket_id", "event_at_rank")
    .orderBy("ticket_id", F.col("event_at_rank").desc(), "id")
    .limit(10)
)
