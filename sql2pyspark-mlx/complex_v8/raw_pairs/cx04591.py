from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_ties = Window.orderBy(F.col("event_at").asc_nulls_last())
w_actor_group_ticket_id_by_eve = Window.partitionBy("actor_group", "ticket_id").orderBy("event_at", "id")
w_all_by_event_at_full = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_ties_2 = Window.orderBy("event_at")
w_actor_group_ticket_id_by_eve_2 = Window.partitionBy("actor_group", "ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_group_by_event_at = Window.partitionBy("actor_group").orderBy("event_at", "id")
w_actor_group_by_event_at_full = Window.partitionBy("actor_group").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_group_ticket_id_by_eve_3 = Window.partitionBy("actor_group", "ticket_id").orderBy(F.col("event_at").desc(), "id")
w_actor_actor_group_by_event_a = Window.partitionBy("actor", "actor_group").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_actor_group_by_event_a_2 = Window.partitionBy("actor", "actor_group").orderBy("event_at", "id")
w_second_event_at_2_year = Window.partitionBy(F.year("second_event_at_2"))
w_event_type_by_event_at_rank = Window.partitionBy("event_type").orderBy("event_at_rank", "id")
w_all_by_event_at_rank_2 = Window.orderBy(F.col("event_at_rank_2").desc(), "id")
w_all_by_event_at_rank_2_prior = Window.orderBy(F.col("event_at_rank_2").desc(), "id").rowsBetween(-1, -1)
w_all_by_event_at_rank_2_runni = Window.orderBy(F.col("event_at_rank_2").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = (
    df_ticket_events
    .filter((F.col("event_at") >= F.lit("2025-01-25 14:00:00").cast("timestamp")) | (F.col("event_type").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("actor").isin("customer", "agent"), "group_a").when(F.col("actor") == "system", "group_b").otherwise("other").alias("actor_group"),
    )
)

with_windows = (
    scoped
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        F.rank().over(w_all_by_event_at_ties).alias("event_at_rank"),
        F.lag("event_at").over(w_actor_group_ticket_id_by_eve).alias("prev_event_at"),
        F.nth_value("event_at", 2).over(w_all_by_event_at_full).alias("second_event_at"),
        F.rank().over(w_all_by_event_at_ties_2).alias("event_at_rank_2"),
        F.first("event_at", ignorenulls=True).over(w_actor_group_ticket_id_by_eve_2).alias("first_known_event_at"),
        F.round(F.percent_rank().over(w_actor_group_by_event_at), 3).alias("event_at_percent_rank"),
        F.first("event_at").over(w_actor_group_by_event_at_full).alias("first_event_at"),
        F.lag("event_at").over(w_actor_group_ticket_id_by_eve_3).alias("prev_event_at_2"),
        F.nth_value("event_at", 2).over(w_actor_actor_group_by_event_a).alias("second_event_at_2"),
        F.row_number().over(w_actor_group_by_event_at).alias("event_at_seq"),
        F.dense_rank().over(w_actor_actor_group_by_event_a_2).alias("event_at_dense_rank"),
    )
)

first_rows = (
    with_windows
    .filter((F.col("event_at_seq") <= 2) & (F.hour(F.col("prev_event_at_2")).between(6, 10)))
)

windowed = (
    first_rows
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "event_at_rank",
        "event_at_rank_2",
        "event_at_percent_rank",
        "first_event_at",
        "prev_event_at_2",
        "second_event_at_2",
        "event_at_dense_rank",
        F.round(F.col("event_at_rank_2") - F.avg("event_at_rank_2").over(w_second_event_at_2_year), 2).alias("event_at_rank_2_vs_second_event_at_2_year_avg"),
        F.round(F.col("event_at_rank") / F.nullif(F.sum("event_at_rank").over(w_second_event_at_2_year), F.lit(0)), 4).alias("event_at_rank_share_of_second_event_at_2_year"),
        F.rank().over(w_event_type_by_event_at_rank).alias("event_at_rank_rank"),
        F.rank().over(w_all_by_event_at_rank_2).alias("event_at_rank_2_rank"),
        F.round(F.percent_rank().over(w_event_type_by_event_at_rank), 3).alias("event_at_rank_percent_rank"),
        F.round(F.avg("event_at_rank_2").over(w_all_by_event_at_rank_2_prior), 2).alias("event_at_rank_2_avg_1_prior"),
        F.first("event_at_rank_2", ignorenulls=True).over(w_all_by_event_at_rank_2_runni).alias("first_known_event_at_rank_2"),
        F.round(F.col("event_at_seq") / F.nullif(F.sum("event_at_seq").over(w_second_event_at_2_year), F.lit(0)), 4).alias("event_at_seq_share_of_second_event_at_2_year"),
    )
)

result = (
    windowed
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "event_at_rank",
        "event_at_rank_2",
        "event_at_percent_rank",
        "first_event_at",
        "prev_event_at_2",
        "second_event_at_2",
        "event_at_dense_rank",
        "event_at_rank_2_vs_second_event_at_2_year_avg",
        "event_at_rank_share_of_second_event_at_2_year",
        "event_at_rank_rank",
        "event_at_rank_2_rank",
        "event_at_rank_2_avg_1_prior",
        "first_known_event_at_rank_2",
        "event_at_seq_share_of_second_event_at_2_year",
    )
)
