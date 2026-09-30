from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_full = Window.orderBy(F.col("event_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_event_type_by_even = Window.partitionBy("ticket_id", "event_type").orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_full_2 = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_by_event_at = Window.partitionBy("event_type").orderBy("event_at", "id")
w_event_type_by_event_at_2 = Window.partitionBy("event_type").orderBy(F.col("event_at").desc_nulls_first(), "id")
w_all_by_event_at_ties = Window.orderBy(F.col("event_at").desc_nulls_first())
w_all_by_event_at = Window.orderBy(F.col("event_at").desc(), "id")
w_event_type_by_event_at_ties = Window.partitionBy("event_type").orderBy("event_at")
w_all_by_event_at_2 = Window.orderBy(F.col("event_at").desc_nulls_first(), "id")
w_all_by_event_at_running = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_events = (
    df_ticket_events
    .filter(F.col("ticket_id") >= 5)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat(F.col("event_type"), F.lit("#"), F.col("id").cast("string")).alias("event_type_ref"),
    )
)

with_parent = (
    recent_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.array_contains(F.col("t.tags"), "bug"))
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_type_ref",
        F.col("t.tags").alias("ticket_tags"),
    )
)

with_trends = (
    with_parent
    .select(
        "*",
        F.first("event_at").over(w_all_by_event_at_full).alias("first_event_at"),
        F.nth_value("event_at", 2).over(w_ticket_id_event_type_by_even).alias("second_event_at"),
        F.first("event_at").over(w_all_by_event_at_full_2).alias("first_event_at_2"),
        F.ntile(4).over(w_event_type_by_event_at).alias("event_at_quartile"),
        F.lag("event_at", 2).over(w_event_type_by_event_at_2).alias("prev2_event_at"),
        F.round(F.percent_rank().over(w_all_by_event_at_ties), 3).alias("event_at_percent_rank"),
        F.ntile(5).over(w_all_by_event_at).alias("event_at_quintile"),
        F.nth_value("event_at", 2).over(w_all_by_event_at_full_2).alias("second_event_at_2"),
        F.rank().over(w_event_type_by_event_at_ties).alias("event_at_rank"),
        F.rank().over(w_all_by_event_at_2).alias("event_at_rank_2"),
        F.first("event_at", ignorenulls=True).over(w_all_by_event_at_running).alias("first_known_event_at"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "event_type_ref",
        "ticket_tags",
        "first_event_at",
        "second_event_at",
        "first_event_at_2",
        "event_at_quartile",
        "prev2_event_at",
        "event_at_quintile",
        "second_event_at_2",
        "event_at_rank_2",
        "first_known_event_at",
        F.greatest(F.col("event_at_percent_rank"), F.col("event_at_rank")).alias("greatest_event_at_percent_rank_event_at_rank"),
    )
    .orderBy(F.col("first_known_event_at").desc_nulls_first(), "event_type_ref")
)
