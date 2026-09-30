from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_ties = Window.orderBy("event_at")
w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_event_type_by_event_at = Window.partitionBy("event_type").orderBy(F.col("event_at").asc_nulls_last(), "id")
w_event_type_by_event_at_runni = Window.partitionBy("event_type").orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_actor_by_event_at = Window.partitionBy("ticket_id", "actor").orderBy("event_at", "id")
w_all_by_event_at = Window.orderBy("event_at", "id")
w_ticket_id_actor_by_event_at_2 = Window.partitionBy("ticket_id", "actor").orderBy("event_at")
w_all_by_event_at_full = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_by_event_at_ties = Window.partitionBy("ticket_id").orderBy("event_at")

filtered = df_ticket_events.filter(~F.col("event_type").isin("assigned", "reopened"))

sequenced = (
    filtered
    .select(
        "*",
        F.dense_rank().over(w_all_by_event_at_ties).alias("event_at_dense_rank"),
        F.dense_rank().over(w_ticket_id_by_event_at).alias("event_at_dense_rank_2"),
        F.row_number().over(w_event_type_by_event_at).alias("event_at_seq"),
        F.first("event_at", ignorenulls=True).over(w_event_type_by_event_at_runni).alias("first_known_event_at"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_ticket_id_actor_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.round(F.percent_rank().over(w_all_by_event_at), 3).alias("event_at_percent_rank"),
        F.lead("event_at").over(w_all_by_event_at).alias("next_event_at"),
        F.round(F.cume_dist().over(w_ticket_id_actor_by_event_at_2), 3).alias("event_at_cume_dist"),
        F.ntile(5).over(w_all_by_event_at).alias("event_at_quintile"),
        F.last("event_at", ignorenulls=True).over(w_event_type_by_event_at_runni).alias("last_known_event_at"),
        F.last("event_at").over(w_all_by_event_at_full).alias("last_event_at"),
        F.row_number().over(w_all_by_event_at).alias("rn"),
        F.row_number().over(w_ticket_id_actor_by_event_at).alias("rn_2"),
        F.round(F.percent_rank().over(w_all_by_event_at_ties), 3).alias("event_at_percent_rank_2"),
        F.round(F.cume_dist().over(w_ticket_id_by_event_at_ties), 3).alias("event_at_cume_dist_2"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "event_at_dense_rank",
        "event_at_dense_rank_2",
        "event_at_seq",
        "first_known_event_at",
        "minutes_since_prev_event_at",
        "event_at_percent_rank",
        "next_event_at",
        "event_at_cume_dist",
        "event_at_quintile",
        "last_known_event_at",
        "last_event_at",
        "rn",
        "rn_2",
        "event_at_percent_rank_2",
        "event_at_cume_dist_2",
        (F.col("event_at_dense_rank_2") + F.col("event_at_cume_dist_2")).alias("event_at_dense_rank_2_plus_event_at_cume_dist_2"),
    )
    .orderBy(
        F.col("event_at").desc_nulls_first(),
        F.col("event_at_percent_rank_2").desc(),
    )
)
