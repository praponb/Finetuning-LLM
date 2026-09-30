from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_ties = Window.orderBy("event_at")
w_all_by_event_at = Window.orderBy("event_at", "id")
w_actor_event_type_or_unknown = Window.partitionBy("actor", "event_type_or_unknown").orderBy(F.col("event_at").desc_nulls_first(), "id")
w_event_type_or_unknown_ticket = Window.partitionBy("event_type_or_unknown", "ticket_id").orderBy("event_at", "id")
w_event_type_or_unknown_ticket_2 = Window.partitionBy("event_type_or_unknown", "ticket_id").orderBy("event_at")
w_actor_event_type_or_unknown_2 = Window.partitionBy("actor", "event_type_or_unknown").orderBy(F.col("event_at").desc_nulls_first())
w_actor_event_type_or_unknown_3 = Window.partitionBy("actor", "event_type_or_unknown").orderBy(F.col("event_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_event_at_running = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_at_dense_rank_3_by_eve = Window.partitionBy("event_at_dense_rank_3").orderBy("event_at_dense_rank_3", "id")
w_event_at_seq = Window.partitionBy("event_at_seq")

base = (
    df_ticket_events
    .filter((F.col("event_type").isin("assigned", "created", "reopened")) & (F.hour(F.col("event_at")).between(9, 15)))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
    )
)

metrics = (
    base
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.dense_rank().over(w_all_by_event_at_ties).alias("event_at_dense_rank"),
        F.row_number().over(w_all_by_event_at).alias("event_at_seq"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_actor_event_type_or_unknown))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.ntile(4).over(w_actor_event_type_or_unknown).alias("event_at_quartile"),
        F.lag("event_at", 2).over(w_actor_event_type_or_unknown).alias("prev2_event_at"),
        F.lead("event_at", 3).over(w_event_type_or_unknown_ticket).alias("next3_event_at"),
        F.dense_rank().over(w_event_type_or_unknown_ticket_2).alias("event_at_dense_rank_2"),
        F.round(F.cume_dist().over(w_actor_event_type_or_unknown_2), 3).alias("event_at_cume_dist"),
        F.first("event_at", ignorenulls=True).over(w_actor_event_type_or_unknown_3).alias("first_known_event_at"),
        F.dense_rank().over(w_actor_event_type_or_unknown).alias("event_at_dense_rank_3"),
        F.last("event_at", ignorenulls=True).over(w_all_by_event_at_running).alias("last_known_event_at"),
        F.lead("event_at").over(w_actor_event_type_or_unknown).alias("next_event_at"),
    )
)

first_rows = metrics.filter(F.col("event_at_dense_rank_2") <= 5)

windowed = (
    first_rows
    .select(
        "*",
        F.row_number().over(w_event_at_dense_rank_3_by_eve).alias("rn"),
        F.ntile(10).over(w_event_at_dense_rank_3_by_eve).alias("event_at_dense_rank_3_decile"),
        F.count(F.lit(1)).over(w_event_at_seq).alias("rows_in_event_at_seq"),
    )
)

result = (
    windowed
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "event_at_dense_rank",
        "event_at_quartile",
        "prev2_event_at",
        "next3_event_at",
        "event_at_dense_rank_2",
        "event_at_cume_dist",
        "first_known_event_at",
        "event_at_dense_rank_3",
        "last_known_event_at",
        "next_event_at",
        "rn",
        "event_at_dense_rank_3_decile",
        "rows_in_event_at_seq",
        F.round(F.col("event_at_cume_dist") * 1.07, 2).alias("event_at_cume_dist_adj"),
    )
    .orderBy(F.col("prev2_event_at").desc())
)
