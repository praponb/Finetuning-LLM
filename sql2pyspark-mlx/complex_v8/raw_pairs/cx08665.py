from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type = Window.partitionBy("event_type").orderBy("event_at", "id")
w_event_type_running = Window.partitionBy("event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_event_at_running = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_event_at_ties = Window.orderBy("event_at")
w_all_by_event_at = Window.orderBy("event_at", "id")
w_event_type_prev_event_at_to = Window.partitionBy("event_type", F.to_date("prev_event_at"))

base = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_prev = base.withColumn("prev_event_at", F.lag("event_at").over(w_event_type))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_event_at").isNull() | ((F.unix_timestamp("event_at") - F.unix_timestamp("prev_event_at")) / 60 > 120), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_event_type_running))

ranked = (
    sessionized
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "session_id",
        F.min("session_id").over(w_all_by_event_at_running).alias("min_session_id_so_far"),
        F.rank().over(w_all_by_event_at_ties).alias("event_at_rank"),
        F.ntile(3).over(w_all_by_event_at).alias("event_at_tercile"),
        F.rank().over(w_all_by_event_at).alias("event_at_rank_2"),
        F.count(F.lit(1)).over(w_event_type_prev_event_at_to).alias("rows_in_event_type_prev_event_at_to_date"),
        F.max("is_new_session").over(w_event_type_prev_event_at_to).alias("event_type_prev_event_at_to_date_max_is_new_sess"),
    )
)

result = (
    ranked
    .select(
        "id",
        "ticket_id",
        "event_at",
        "session_id",
        "event_at_rank",
        "event_at_tercile",
        "event_at_rank_2",
        "rows_in_event_type_prev_event_at_to_date",
        "event_type_prev_event_at_to_date_max_is_new_sess",
        F.abs(F.col("event_at_tercile") - 4).alias("event_at_tercile_distance"),
        F.abs(F.col("session_id") - 2).alias("session_id_distance"),
    )
    .orderBy(F.col("session_id").desc())
)
