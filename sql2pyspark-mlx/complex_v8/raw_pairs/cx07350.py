from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_event_type_by_even = Window.partitionBy("ticket_id", "event_type").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_event_type_by_even_2 = Window.partitionBy("ticket_id", "event_type").orderBy(F.col("event_at").desc(), "id")
w_ticket_id_by_event_at_runnin = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_by_event_at_full = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at = Window.orderBy("event_at", "id")
w_event_type_by_event_at_ties = Window.partitionBy("event_type").orderBy("event_at")
w_event_type_by_event_at = Window.partitionBy("event_type").orderBy("event_at", "id")

filtered = (
    df_ticket_events
    .filter((F.col("actor").isNotNull()) & (F.col("event_type") == "replied"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_windows = (
    filtered
    .select(
        "*",
        F.last("event_at", ignorenulls=True).over(w_ticket_id_event_type_by_even).alias("last_known_event_at"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_ticket_id_event_type_by_even_2))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.last("event_at", ignorenulls=True).over(w_ticket_id_by_event_at_runnin).alias("last_known_event_at_2"),
        F.nth_value("event_at", 2).over(w_ticket_id_by_event_at_full).alias("second_event_at"),
        F.round(F.cume_dist().over(w_all_by_event_at), 3).alias("event_at_cume_dist"),
        F.dense_rank().over(w_ticket_id_event_type_by_even_2).alias("event_at_dense_rank"),
        F.dense_rank().over(w_event_type_by_event_at_ties).alias("event_at_dense_rank_2"),
        F.rank().over(w_all_by_event_at).alias("event_at_rank"),
        F.row_number().over(w_all_by_event_at).alias("seq_no"),
        F.lag("event_at", 3).over(w_event_type_by_event_at).alias("prev3_event_at"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "event_at",
        "event_type",
        "last_known_event_at",
        "minutes_since_prev_event_at",
        "second_event_at",
        "event_at_cume_dist",
        "event_at_dense_rank",
        "event_at_dense_rank_2",
        "event_at_rank",
        F.when(F.col("event_type").isin("created", "resolved"), "group_a").when(F.col("event_type") == "replied", "group_b").otherwise("other").alias("event_type_group"),
        F.concat_ws(" / ", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
    )
    .orderBy(F.col("event_type_group").desc(), "event_type")
)
