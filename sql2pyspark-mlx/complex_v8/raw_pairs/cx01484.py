from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_actor_by_event_at = Window.partitionBy("ticket_id", "actor").orderBy("event_at", "id")
w_ticket_id_event_type_by_even = Window.partitionBy("ticket_id", "event_type").orderBy(F.col("event_at").asc_nulls_last(), "id")
w_event_type_ticket_id_by_even = Window.partitionBy("event_type", "ticket_id").orderBy(F.col("event_at").desc(), "id")
w_ticket_id_actor_by_event_at_2 = Window.partitionBy("ticket_id", "actor").orderBy(F.col("event_at").asc_nulls_last())
w_actor_by_event_at_full = Window.partitionBy("actor").orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_by_event_at_ties = Window.partitionBy("actor").orderBy(F.col("event_at").asc_nulls_last())
w_all_by_event_at_rank = Window.orderBy(F.col("event_at_rank").desc(), "id")
w_event_at_rank_by_next_event = Window.partitionBy("event_at_rank").orderBy(F.col("next_event_at").desc(), "id")
w_event_at_seq_event_at_tercil = Window.partitionBy("event_at_seq", "event_at_tercile").orderBy(F.col("event_at_dense_rank").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_seq_event_at_tercil_2 = Window.partitionBy("event_at_seq", "event_at_tercile").orderBy(F.col("event_at_dense_rank").desc_nulls_first(), "id")
w_event_at_rank_by_next_event_2 = Window.partitionBy("event_at_rank").orderBy(F.col("next_event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = df_ticket_events.filter(F.col("event_type").isin("escalated", "replied"))

sequenced = (
    scoped
    .select(
        "*",
        F.lead("event_at").over(w_ticket_id_actor_by_event_at).alias("next_event_at"),
        F.row_number().over(w_ticket_id_event_type_by_even).alias("event_at_seq"),
        F.ntile(10).over(w_event_type_ticket_id_by_even).alias("event_at_decile"),
        F.ntile(3).over(w_event_type_ticket_id_by_even).alias("event_at_tercile"),
        F.rank().over(w_ticket_id_actor_by_event_at_2).alias("event_at_rank"),
        F.dense_rank().over(w_ticket_id_actor_by_event_at).alias("event_at_dense_rank"),
        F.first("event_at").over(w_actor_by_event_at_full).alias("first_event_at"),
        F.rank().over(w_actor_by_event_at_ties).alias("event_at_rank_2"),
    )
)

metrics = (
    sequenced
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "next_event_at",
        "event_at_seq",
        "event_at_tercile",
        "event_at_rank",
        "first_event_at",
        "event_at_rank_2",
        F.lead("event_at_dense_rank").over(w_all_by_event_at_rank).alias("next_event_at_dense_rank"),
        (F.col("event_at_rank_2") - F.lag("event_at_rank_2").over(w_event_at_rank_by_next_event)).alias("event_at_rank_2_change"),
        F.nth_value("event_at_dense_rank", 2).over(w_event_at_seq_event_at_tercil).alias("second_event_at_dense_rank"),
        F.lag("event_at_decile").over(w_event_at_seq_event_at_tercil_2).alias("prev_event_at_decile"),
        F.sum("event_at_seq").over(w_event_at_rank_by_next_event_2).alias("running_event_at_seq"),
    )
)

result = (
    metrics
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "next_event_at",
        "event_at_tercile",
        "event_at_rank",
        "event_at_rank_2",
        "next_event_at_dense_rank",
        "event_at_rank_2_change",
        F.when(F.col("actor").isin("agent", "system"), "group_a").when(F.col("actor") == "customer", "group_b").otherwise("other").alias("actor_group"),
        F.substring(F.trim(F.col("actor")), 1, 2).alias("actor_prefix"),
    )
    .orderBy(F.col("event_at_tercile").asc_nulls_last())
)
