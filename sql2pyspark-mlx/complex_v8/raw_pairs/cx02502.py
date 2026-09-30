from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_ticket_id = Window.partitionBy("actor", "ticket_id")
w_actor = Window.partitionBy("actor")
w_all_by_event_at = Window.orderBy(F.col("event_at").desc(), "id")
w_all_by_days_since_event_at = Window.orderBy(F.col("days_since_event_at").desc_nulls_first(), "id")
w_ticket_id_by_days_since_even = Window.partitionBy("ticket_id").orderBy("days_since_event_at")
w_actor_by_event_at = Window.partitionBy("actor").orderBy(F.col("event_at").desc(), "id")
w_actor_by_event_at_moving_2 = Window.partitionBy("actor").orderBy(F.col("event_at").desc(), "id").rowsBetween(-2, Window.currentRow)
w_event_at_seq_by_days_since_e = Window.partitionBy("event_at_seq").orderBy(F.col("days_since_event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_event_at_month = Window.partitionBy("actor", F.date_trunc("month", "event_at"))
w_event_type_by_event_at_runni = Window.partitionBy("event_type").orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type_by_event_at_movin = Window.partitionBy("event_type").orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(-1, Window.currentRow)
w_event_type_by_event_at = Window.partitionBy("event_type").orderBy(F.col("event_at").asc_nulls_last(), "id")

valid = (
    df_ticket_events
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

ranked = (
    valid
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "days_since_event_at",
        F.round(F.avg("days_since_event_at").over(w_actor_ticket_id), 2).alias("actor_ticket_id_avg_days_since_event_at"),
        F.round(F.col("days_since_event_at") - F.avg("days_since_event_at").over(w_actor), 2).alias("days_since_event_at_vs_actor_avg"),
        F.row_number().over(w_all_by_event_at).alias("event_at_seq"),
        F.count(F.lit(1)).over(w_actor).alias("rows_in_actor"),
        F.lead("days_since_event_at").over(w_all_by_days_since_event_at).alias("next_days_since_event_at"),
        F.sum("days_since_event_at").over(w_actor_ticket_id).alias("actor_ticket_id_sum_days_since_event_at"),
        F.rank().over(w_ticket_id_by_days_since_even).alias("days_since_event_at_rank"),
        F.lag("days_since_event_at", 1, 0).over(w_actor_by_event_at).alias("prev_days_since_event_at"),
        F.max("days_since_event_at").over(w_actor_by_event_at_moving_2).alias("days_since_event_at_max_3"),
        F.sum("days_since_event_at").over(w_actor).alias("actor_sum_days_since_event_at"),
    )
)

with_trends = (
    ranked
    .select(
        "*",
        F.last("days_since_event_at").over(w_event_at_seq_by_days_since_e).alias("last_days_since_event_at"),
        F.round(F.col("next_days_since_event_at") - F.avg("next_days_since_event_at").over(w_actor_event_at_month), 2).alias("next_days_since_event_at_vs_actor_event_at_month"),
        F.sum("days_since_event_at_max_3").over(w_actor_event_at_month).alias("actor_event_at_month_sum_days_since_event_at_max"),
        F.first("next_days_since_event_at", ignorenulls=True).over(w_event_type_by_event_at_runni).alias("first_known_next_days_since_event_at"),
        F.max("rows_in_actor").over(w_event_type_by_event_at_movin).alias("rows_in_actor_max_2"),
        F.round(F.cume_dist().over(w_event_type_by_event_at), 3).alias("event_at_cume_dist"),
    )
)

result = (
    with_trends
    .select("actor", "next_days_since_event_at")
    .distinct()
    .orderBy("next_days_since_event_at")
)
