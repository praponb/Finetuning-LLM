from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_or_unknown_by_event_at = Window.partitionBy("actor_or_unknown").orderBy("event_at", "id")
w_actor_or_unknown_by_event_at_2 = Window.partitionBy("actor_or_unknown").orderBy(F.col("event_at").asc_nulls_last(), "id")
w_actor_or_unknown_by_event_at_3 = Window.partitionBy("actor_or_unknown").orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_ticket_id_by_even = Window.partitionBy("event_type", "ticket_id").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_or_unknown_by_event_at_4 = Window.partitionBy("actor_or_unknown").orderBy(F.col("event_at").asc_nulls_last())

base_events = (
    df_ticket_events
    .filter(F.col("event_type") == "replied")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
    )
)

windowed = (
    base_events
    .select(
        "*",
        F.row_number().over(w_actor_or_unknown_by_event_at).alias("event_at_seq"),
        F.round(F.percent_rank().over(w_actor_or_unknown_by_event_at_2), 3).alias("event_at_percent_rank"),
        F.lag("event_at").over(w_actor_or_unknown_by_event_at).alias("prev_event_at"),
        F.row_number().over(w_actor_or_unknown_by_event_at_2).alias("rn"),
        F.nth_value("event_at", 2).over(w_actor_or_unknown_by_event_at_3).alias("second_event_at"),
        F.nth_value("event_at", 2).over(w_event_type_ticket_id_by_even).alias("second_event_at_2"),
        F.dense_rank().over(w_actor_or_unknown_by_event_at_4).alias("event_at_dense_rank"),
    )
)

top_rows = windowed.filter((F.col("rn") <= 5) & (F.col("ticket_id") >= 26))

result = (
    top_rows
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor_or_unknown",
        "event_at_percent_rank",
        "rn",
        "second_event_at",
        "second_event_at_2",
        F.when(F.col("actor_or_unknown").isin("customer", "system"), "group_a").when(F.col("actor_or_unknown") == "UNKNOWN", "group_b").otherwise("other").alias("actor_or_unknown_group"),
    )
    .orderBy(
        F.col("event_at_percent_rank").desc_nulls_first(),
        F.col("second_event_at_2").asc(),
        "id",
    )
    .limit(10)
)
