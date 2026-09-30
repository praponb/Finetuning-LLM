from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_by_event_at_full = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_by_event_at_ties = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc())
w_all_by_event_at = Window.orderBy(F.col("event_at").desc(), "id")
w_ticket_id_actor_by_event_at = Window.partitionBy("ticket_id", "actor").orderBy(F.col("event_at").desc(), "id")
w_event_at_month_event_type_by = Window.partitionBy(F.date_trunc("month", "event_at"), "event_type").orderBy("event_at", "id")
w_actor_event_type_by_event_at = Window.partitionBy("actor", "event_type").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_actor_by_event_at_2 = Window.partitionBy("ticket_id", "actor").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_at_month_event_type_by_2 = Window.partitionBy(F.date_trunc("month", "event_at"), "event_type").orderBy("event_at")
w_actor_event_type_by_event_at_2 = Window.partitionBy("actor", "event_type").orderBy(F.col("event_at").desc())
w_actor_event_type_by_event_at_3 = Window.partitionBy("actor", "event_type").orderBy(F.col("event_at").desc(), "id")
w_last_event_at_month_by_event = Window.partitionBy(F.month("last_event_at")).orderBy("event_at_dense_rank_3")
w_seq_no_event_at_rank_by_firs = Window.partitionBy("seq_no", "event_at_rank").orderBy("first_event_at", "id")
w_event_type = Window.partitionBy("event_type")

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

ranked = (
    base
    .select(
        "*",
        F.last("event_at").over(w_ticket_id_by_event_at_full).alias("last_event_at"),
        F.dense_rank().over(w_ticket_id_by_event_at_ties).alias("event_at_dense_rank"),
        F.dense_rank().over(w_all_by_event_at).alias("event_at_dense_rank_2"),
        F.ntile(3).over(w_ticket_id_actor_by_event_at).alias("event_at_tercile"),
        F.row_number().over(w_event_at_month_event_type_by).alias("seq_no"),
        F.rank().over(w_ticket_id_by_event_at_ties).alias("event_at_rank"),
        F.first("event_at").over(w_actor_event_type_by_event_at).alias("first_event_at"),
        F.last("event_at", ignorenulls=True).over(w_ticket_id_actor_by_event_at_2).alias("last_known_event_at"),
        F.dense_rank().over(w_event_at_month_event_type_by_2).alias("event_at_dense_rank_3"),
        F.round(F.percent_rank().over(w_actor_event_type_by_event_at_2), 3).alias("event_at_percent_rank"),
        F.lead("event_at", 2).over(w_actor_event_type_by_event_at_3).alias("next2_event_at"),
    )
)

metrics = (
    ranked
    .select(
        "*",
        F.round(F.cume_dist().over(w_last_event_at_month_by_event), 3).alias("event_at_dense_rank_3_cume_dist"),
        F.rank().over(w_seq_no_event_at_rank_by_firs).alias("first_event_at_rank"),
        F.max("event_at_rank").over(w_event_type).alias("event_type_max_event_at_rank"),
        F.count(F.lit(1)).over(w_event_type).alias("rows_in_event_type"),
    )
)

result = (
    metrics
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "last_event_at",
        "event_at_dense_rank",
        "event_at_dense_rank_2",
        "seq_no",
        "event_at_rank",
        "first_event_at",
        "event_at_dense_rank_3",
        "next2_event_at",
        "first_event_at_rank",
        "event_type_max_event_at_rank",
        "rows_in_event_type",
    )
    .orderBy("event_type_max_event_at_rank", "id")
    .limit(10)
)
