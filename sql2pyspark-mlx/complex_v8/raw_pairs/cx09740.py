from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_by_event_at = Window.partitionBy("event_type").orderBy(F.col("event_at").asc_nulls_last(), "id")
w_event_type_by_event_at_full = Window.partitionBy("event_type").orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_dedup = Window.partitionBy("actor", "ticket_id").orderBy(F.col("event_at_seq").desc(), F.col("id").desc())
w_dedup_2 = Window.partitionBy("actor", "ticket_id").orderBy(F.col("second_event_at").desc(), F.col("id").desc())
w_all_by_prev_event_at = Window.orderBy(F.col("prev_event_at").asc_nulls_last(), "id")
w_event_at_seq_actor = Window.partitionBy("event_at_seq", "actor")
w_all_by_prev_event_at_ties = Window.orderBy(F.col("prev_event_at").asc_nulls_last())

scoped_events = (
    df_ticket_events
    .filter(F.col("event_at") < F.lit("2025-02-24 13:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

ranked = (
    scoped_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        F.lag("event_at").over(w_event_type_by_event_at).alias("prev_event_at"),
        F.row_number().over(w_event_type_by_event_at).alias("event_at_seq"),
        F.nth_value("event_at", 2).over(w_event_type_by_event_at_full).alias("second_event_at"),
    )
)

qualified = (
    ranked.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type") == "assigned") & (F.col("t.event_at") < F.lit("2025-02-12 13:00:00").cast("timestamp")), "left_anti")
)

unique_rows = (
    qualified
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

unique_rows_2 = (
    unique_rows
    .withColumn("rn", F.row_number().over(w_dedup_2))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_windows = (
    unique_rows_2
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_prev_event_at), 3).alias("prev_event_at_cume_dist"),
        F.count(F.lit(1)).over(w_event_at_seq_actor).alias("rows_in_event_at_seq_actor"),
        F.round(F.cume_dist().over(w_all_by_prev_event_at_ties), 3).alias("prev_event_at_cume_dist_2"),
        F.lag("second_event_at", 2).over(w_all_by_prev_event_at).alias("prev2_second_event_at"),
    )
)

result = (
    with_windows
    .filter(F.col("event_at_seq").between(3, 10))
    .select(
        "id",
        "event_at",
        "actor",
        "prev_event_at",
        "second_event_at",
        "prev_event_at_cume_dist",
        "rows_in_event_at_seq_actor",
        "prev_event_at_cume_dist_2",
        "prev2_second_event_at",
        F.round(F.col("prev_event_at_cume_dist") * 1.07, 2).alias("prev_event_at_cume_dist_adj"),
    )
    .orderBy(F.col("prev_event_at_cume_dist").desc())
)
