from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_by_event_at = Window.partitionBy("actor").orderBy("event_at", "id")
w_all_by_event_at_full = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_month_ticket_id_by = Window.partitionBy(F.month("event_at"), "ticket_id").orderBy("event_at")
w_event_at_month_ticket_id_by_2 = Window.partitionBy(F.month("event_at"), "ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_event_at = Window.orderBy(F.col("event_at").desc(), "id")
w_ticket_id_by_event_at_full = Window.partitionBy("ticket_id").orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_running = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_at_month_ticket_id_by_3 = Window.partitionBy(F.month("event_at"), "ticket_id").orderBy("event_at", "id")
w_event_at_dense_rank = Window.partitionBy("event_at_dense_rank")
w_event_at_dense_rank_by_secon = Window.partitionBy("event_at_dense_rank").orderBy(F.col("second_event_at").desc(), "id")
w_next_event_at_to_date_event = Window.partitionBy(F.to_date("next_event_at"), "event_at_quintile").orderBy("event_at_rank", "id")
w_event_at_dense_rank_by_secon_2 = Window.partitionBy("event_at_dense_rank").orderBy(F.col("second_event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_ticket_events
    .filter((F.col("actor").isNotNull()) & (F.col("event_type").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

windowed = (
    valid
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        F.lag("event_at", 2).over(w_actor_by_event_at).alias("prev2_event_at"),
        F.nth_value("event_at", 2).over(w_all_by_event_at_full).alias("second_event_at"),
        F.rank().over(w_event_at_month_ticket_id_by).alias("event_at_rank"),
        F.lead("event_at").over(w_actor_by_event_at).alias("next_event_at"),
        F.first("event_at", ignorenulls=True).over(w_event_at_month_ticket_id_by_2).alias("first_known_event_at"),
        F.ntile(5).over(w_all_by_event_at).alias("event_at_quintile"),
        F.lag("event_at", 2).over(w_all_by_event_at).alias("prev2_event_at_2"),
        F.last("event_at").over(w_ticket_id_by_event_at_full).alias("last_event_at"),
        F.last("event_at", ignorenulls=True).over(w_all_by_event_at_running).alias("last_known_event_at"),
        F.dense_rank().over(w_event_at_month_ticket_id_by_3).alias("event_at_dense_rank"),
    )
)

windowed_2 = (
    windowed
    .select(
        "id",
        "event_at",
        "actor",
        "second_event_at",
        "event_at_rank",
        "next_event_at",
        "first_known_event_at",
        "prev2_event_at_2",
        "last_event_at",
        "last_known_event_at",
        F.count(F.lit(1)).over(w_event_at_dense_rank).alias("rows_in_event_at_dense_rank"),
        F.round(F.cume_dist().over(w_event_at_dense_rank_by_secon), 3).alias("second_event_at_cume_dist"),
        F.row_number().over(w_next_event_at_to_date_event).alias("rn"),
        F.lag("event_at_dense_rank", 2).over(w_event_at_dense_rank_by_secon).alias("prev2_event_at_dense_rank"),
        F.first("event_at_rank", ignorenulls=True).over(w_event_at_dense_rank_by_secon_2).alias("first_known_event_at_rank"),
    )
)

result = (
    windowed_2
    .filter(F.col("second_event_at_cume_dist") > 0.2)
    .select(
        "id",
        "actor",
        "event_at_rank",
        "next_event_at",
        "first_known_event_at",
        "prev2_event_at_2",
        "last_event_at",
        "last_known_event_at",
        "rows_in_event_at_dense_rank",
        "second_event_at_cume_dist",
        "rn",
        "prev2_event_at_dense_rank",
        F.when(F.col("actor").isin("agent", "system"), "group_a").when(F.col("actor") == "customer", "group_b").otherwise("other").alias("actor_group"),
    )
    .orderBy(F.col("event_at_rank").desc())
)
