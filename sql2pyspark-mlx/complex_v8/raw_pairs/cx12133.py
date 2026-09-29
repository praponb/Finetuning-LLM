from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_full = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at = Window.orderBy("event_at", "id")
w_actor_by_event_at = Window.partitionBy("actor").orderBy("event_at", "id")
w_event_type_event_at_to_date = Window.partitionBy("event_type", F.to_date("event_at")).orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_actor_by_event_at = Window.partitionBy("event_type", "actor").orderBy(F.col("event_at").desc(), "id")
w_event_type_event_at_to_date_2 = Window.partitionBy("event_type", F.to_date("event_at")).orderBy("event_at", "id")
w_all_by_event_at_ties = Window.orderBy("event_at")
w_event_type_actor_by_event_at_2 = Window.partitionBy("event_type", "actor").orderBy(F.col("event_at").desc())
w_event_type_event_at_to_date_3 = Window.partitionBy("event_type", F.to_date("event_at")).orderBy("event_at")
w_prev3_event_at_to_date = Window.partitionBy(F.to_date("prev3_event_at"))
w_actor_by_second_event_at = Window.partitionBy("actor").orderBy("second_event_at", "id")
w_event_type_event_at_rank_4_b = Window.partitionBy("event_type", "event_at_rank_4_bucket")
w_event_at_rank_event_at_rank = Window.partitionBy("event_at_rank", "event_at_rank_3").orderBy("event_at_rank_2", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_next_event_at_ties = Window.orderBy("next_event_at")
w_actor_by_second_event_at_mov = Window.partitionBy("actor").orderBy("second_event_at", "id").rowsBetween(-1, Window.currentRow)

valid = (
    df_ticket_events
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

metrics = (
    valid
    .select(
        "*",
        F.first("event_at").over(w_all_by_event_at_full).alias("first_event_at"),
        F.rank().over(w_all_by_event_at).alias("event_at_rank"),
        F.row_number().over(w_all_by_event_at).alias("seq_no"),
        F.lead("event_at").over(w_actor_by_event_at).alias("next_event_at"),
        F.first("event_at").over(w_event_type_event_at_to_date).alias("first_event_at_2"),
        F.nth_value("event_at", 2).over(w_all_by_event_at_full).alias("second_event_at"),
        F.rank().over(w_event_type_actor_by_event_at).alias("event_at_rank_2"),
        F.lag("event_at", 3).over(w_event_type_event_at_to_date_2).alias("prev3_event_at"),
        F.round(F.cume_dist().over(w_all_by_event_at_ties), 3).alias("event_at_cume_dist"),
        F.rank().over(w_event_type_actor_by_event_at_2).alias("event_at_rank_3"),
        F.rank().over(w_event_type_event_at_to_date_3).alias("event_at_rank_4"),
    )
)

labeled = (
    metrics
    .filter(F.col("event_at_rank") > 5)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "first_event_at",
        "event_at_rank",
        "next_event_at",
        "second_event_at",
        "event_at_rank_2",
        "prev3_event_at",
        "event_at_rank_3",
        "event_at_rank_4",
        F.when(F.col("actor").isin("customer", "agent"), "group_a").when(F.col("actor") == "system", "group_b").otherwise("other").alias("actor_group"),
        (F.floor(F.col("event_at_rank_4") / 2) * 2).alias("event_at_rank_4_bucket"),
    )
)

ranked = (
    labeled
    .select(
        "id",
        "event_at",
        "actor",
        "first_event_at",
        "event_at_rank",
        "event_at_rank_2",
        "prev3_event_at",
        "event_at_rank_3",
        "event_at_rank_4_bucket",
        F.max("event_at_rank_2").over(w_prev3_event_at_to_date).alias("prev3_event_at_to_date_max_event_at_rank_2"),
        F.dense_rank().over(w_actor_by_second_event_at).alias("second_event_at_dense_rank"),
        F.sum("event_at_rank_3").over(w_event_type_event_at_rank_4_b).alias("event_type_event_at_rank_4_bucket_sum_event_at_r"),
        F.count(F.lit(1)).over(w_event_type_event_at_rank_4_b).alias("rows_in_event_type_event_at_rank_4_bucket"),
        F.nth_value("event_at_rank_4", 2).over(w_event_at_rank_event_at_rank).alias("second_event_at_rank_4"),
        F.rank().over(w_all_by_next_event_at_ties).alias("next_event_at_rank"),
        F.round(F.avg("event_at_rank").over(w_actor_by_second_event_at_mov), 2).alias("event_at_rank_avg_2"),
    )
)

result = (
    ranked
    .select(
        "id",
        "first_event_at",
        "event_at_rank",
        "event_at_rank_2",
        "prev3_event_at",
        "event_at_rank_3",
        "event_at_rank_4_bucket",
        "prev3_event_at_to_date_max_event_at_rank_2",
        "second_event_at_rank_4",
        "next_event_at_rank",
    )
    .orderBy("event_at_rank", F.col("event_at_rank_2").desc())
)
