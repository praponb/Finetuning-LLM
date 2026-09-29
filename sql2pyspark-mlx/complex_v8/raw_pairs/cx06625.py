from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at = Window.orderBy("event_at", "id")
w_event_at_to_date_event_type = Window.partitionBy(F.to_date("event_at"), "event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_to_date_event_type_2 = Window.partitionBy(F.to_date("event_at"), "event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type_by_event_at_full = Window.partitionBy("event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_by_event_at = Window.partitionBy("event_type").orderBy("event_at", "id")
w_all_by_event_at_ties = Window.orderBy("event_at")
w_all_by_event_at_full = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_ties_2 = Window.orderBy(F.col("event_at").desc())
w_event_type_ticket_id_by_even = Window.partitionBy("event_type", "ticket_id").orderBy(F.col("event_at").desc(), "id")
w_event_type_by_event_at_ties = Window.partitionBy("event_type").orderBy("event_at")
w_all_by_event_at_2 = Window.orderBy(F.col("event_at").desc(), "id")

scoped_events = (
    df_ticket_events
    .filter(F.col("actor") != "system")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_trends = (
    scoped_events
    .select(
        "*",
        F.lag("event_at").over(w_all_by_event_at).alias("prev_event_at"),
        F.first("event_at").over(w_event_at_to_date_event_type).alias("first_event_at"),
        F.lag("event_at", 2).over(w_all_by_event_at).alias("prev2_event_at"),
        F.first("event_at", ignorenulls=True).over(w_event_at_to_date_event_type_2).alias("first_known_event_at"),
        F.first("event_at").over(w_event_type_by_event_at_full).alias("first_event_at_2"),
        F.lead("event_at", 3).over(w_event_type_by_event_at).alias("next3_event_at"),
        F.round(F.percent_rank().over(w_all_by_event_at_ties), 3).alias("event_at_percent_rank"),
        F.first("event_at").over(w_all_by_event_at_full).alias("first_event_at_3"),
        F.round(F.percent_rank().over(w_event_type_by_event_at), 3).alias("event_at_percent_rank_2"),
        F.round(F.cume_dist().over(w_all_by_event_at_ties_2), 3).alias("event_at_cume_dist"),
        F.lag("event_at").over(w_event_type_ticket_id_by_even).alias("prev_event_at_2"),
        F.rank().over(w_event_type_by_event_at_ties).alias("event_at_rank"),
        F.row_number().over(w_all_by_event_at_2).alias("rn"),
    )
)

enriched = (
    with_trends
    .filter(F.month(F.col("first_known_event_at")).isin(2, 3))
    .select(
        "*",
        F.abs(F.col("event_at_cume_dist") - 0.5).alias("event_at_cume_dist_distance"),
        F.lower(F.trim(F.col("event_type"))).alias("event_type_clean"),
    )
)

result = (
    enriched
    .select(
        "id",
        "ticket_id",
        "event_at",
        "prev_event_at",
        "first_event_at",
        "prev2_event_at",
        "first_known_event_at",
        "first_event_at_2",
        "next3_event_at",
        "event_at_percent_rank",
        "first_event_at_3",
        "event_at_percent_rank_2",
        "event_at_cume_dist",
        "prev_event_at_2",
        "event_at_rank",
        "event_at_cume_dist_distance",
        "event_type_clean",
    )
    .orderBy(F.col("prev_event_at_2").desc(), F.col("id").desc_nulls_first())
)
