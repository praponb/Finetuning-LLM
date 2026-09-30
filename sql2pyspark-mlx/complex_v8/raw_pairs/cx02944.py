from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at = Window.orderBy("event_at", "id")
w_event_type_event_at_to_date = Window.partitionBy("event_type", F.to_date("event_at")).orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_by_event_at = Window.partitionBy("actor").orderBy(F.col("event_at").asc_nulls_last(), "id")
w_event_type_event_at_to_date_2 = Window.partitionBy("event_type", F.to_date("event_at")).orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_by_event_at_full = Window.partitionBy("actor").orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_event_at_to_date_3 = Window.partitionBy("event_type", F.to_date("event_at")).orderBy("event_at", "id")
w_all_by_event_at_ties = Window.orderBy("event_at")

valid = (
    df_ticket_events
    .filter((F.hour(F.col("event_at")).between(9, 12)) | (F.col("event_type").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

ranked = (
    valid
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lead("event_at").over(w_all_by_event_at).alias("next_event_at"),
        F.last("event_at", ignorenulls=True).over(w_event_type_event_at_to_date).alias("last_known_event_at"),
        F.dense_rank().over(w_actor_by_event_at).alias("event_at_dense_rank"),
        F.nth_value("event_at", 2).over(w_event_type_event_at_to_date_2).alias("second_event_at"),
        F.lag("event_at").over(w_actor_by_event_at).alias("prev_event_at"),
        F.nth_value("event_at", 2).over(w_actor_by_event_at_full).alias("second_event_at_3"),
        F.row_number().over(w_event_type_event_at_to_date_3).alias("event_at_seq"),
        F.first("event_at").over(w_event_type_event_at_to_date_2).alias("first_event_at"),
        F.rank().over(w_all_by_event_at_ties).alias("event_at_rank"),
        F.round(F.cume_dist().over(w_all_by_event_at_ties), 3).alias("event_at_cume_dist"),
        F.row_number().over(w_all_by_event_at).alias("rn"),
        F.round(F.percent_rank().over(w_actor_by_event_at), 3).alias("event_at_percent_rank"),
    )
)

totals = (
    ranked
    .filter(F.col("actor") != "agent")
    .groupBy(
        F.date_trunc("month", "prev_event_at").alias("prev_event_at_month"),
        "event_at_seq",
    )
    .agg(
        F.max("second_event_at").alias("last_second_event_at"),
        F.round(F.avg("event_at_rank"), 2).alias("avg_event_at_rank"),
        F.array_sort(F.collect_set("event_type")).alias("event_type_list"),
        F.percentile_approx("event_at_rank", 0.5).alias("median_event_at_rank"),
    )
)

result = (
    totals
    .select(
        "prev_event_at_month",
        "event_at_seq",
        "last_second_event_at",
        "avg_event_at_rank",
        "median_event_at_rank",
        F.year(F.col("prev_event_at_month")).alias("prev_event_at_month_year"),
    )
    .orderBy(F.col("median_event_at_rank").asc(), "avg_event_at_rank")
)
