from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_by_event_at = Window.partitionBy("actor").orderBy(F.col("event_at").asc_nulls_last(), "id")
w_event_at_year_by_event_at = Window.partitionBy(F.year("event_at")).orderBy(F.col("event_at").desc(), "id")
w_event_at_month_by_event_at = Window.partitionBy(F.month("event_at")).orderBy(F.col("event_at").asc_nulls_last(), "id")
w_event_at_month_by_event_at_t = Window.partitionBy(F.month("event_at")).orderBy(F.col("event_at").asc_nulls_last())
w_event_type_ticket_id_by_even = Window.partitionBy("event_type", "ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_ticket_id_by_even_2 = Window.partitionBy("event_type", "ticket_id").orderBy("event_at")
w_actor_by_event_at_full = Window.partitionBy("actor").orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_by_event_at_full = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned_events = (
    df_ticket_events
    .filter(F.col("ticket_id") >= 15)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

ranked = (
    cleaned_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lead("event_at").over(w_actor_by_event_at).alias("next_event_at"),
        F.dense_rank().over(w_actor_by_event_at).alias("event_at_dense_rank"),
        F.ntile(5).over(w_event_at_year_by_event_at).alias("event_at_quintile"),
        F.row_number().over(w_event_at_month_by_event_at).alias("event_at_seq"),
        F.rank().over(w_event_at_month_by_event_at_t).alias("event_at_rank"),
        F.nth_value("event_at", 2).over(w_event_type_ticket_id_by_even).alias("second_event_at"),
        F.round(F.cume_dist().over(w_event_type_ticket_id_by_even_2), 3).alias("event_at_cume_dist"),
        F.first("event_at").over(w_actor_by_event_at_full).alias("first_event_at"),
        F.row_number().over(w_actor_by_event_at).alias("seq_no"),
        F.round(F.cume_dist().over(w_event_at_month_by_event_at_t), 3).alias("event_at_cume_dist_2"),
        F.nth_value("event_at", 2).over(w_actor_by_event_at_full).alias("second_event_at_2"),
        F.nth_value("event_at", 2).over(w_ticket_id_by_event_at_full).alias("second_event_at_3"),
        F.ntile(5).over(w_actor_by_event_at).alias("event_at_quintile_2"),
    )
)

result = (
    ranked
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "next_event_at",
        "event_at_dense_rank",
        "event_at_quintile",
        "event_at_seq",
        "event_at_rank",
        "second_event_at",
        "event_at_cume_dist",
        "seq_no",
        "event_at_cume_dist_2",
        "second_event_at_2",
        "second_event_at_3",
    )
    .orderBy(F.col("second_event_at_3").asc_nulls_last())
)
