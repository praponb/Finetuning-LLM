from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_full = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at = Window.orderBy(F.col("event_at").desc(), "id")
w_event_type_by_event_at_ties = Window.partitionBy("event_type").orderBy(F.col("event_at").desc())

valid_events = (
    df_ticket_events
    .filter(F.col("event_type") == "replied")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.trim(F.col("event_type"))).alias("event_type_clean"),
    )
)

metrics = (
    valid_events
    .select(
        "id",
        "ticket_id",
        "event_type",
        "event_type_clean",
        F.last("event_at").over(w_all_by_event_at_full).alias("last_event_at"),
        F.nth_value("event_at", 2).over(w_all_by_event_at_full).alias("second_event_at"),
        F.row_number().over(w_all_by_event_at).alias("rn"),
        F.rank().over(w_event_type_by_event_at_ties).alias("event_at_rank"),
    )
)

result = (
    metrics
    .groupBy("last_event_at", "event_at_rank")
    .pivot("event_type", ["escalated", "resolved", "assigned", "reopened"])
    .agg(F.sum("rn"))
    .orderBy("last_event_at", "event_at_rank")
)
