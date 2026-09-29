from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc(), "id")
w_all_by_event_at = Window.orderBy(F.col("event_at").desc(), "id")
w_all_by_event_at_full = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_ticket_id_by_even = Window.partitionBy("event_type", "ticket_id").orderBy("event_at", "id")
w_ticket_id_by_event_at_runnin = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_event_at_running = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type_ticket_id_by_even_2 = Window.partitionBy("event_type", "ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_seq_ties = Window.orderBy(F.col("event_at_seq").desc())
w_all_by_event_at_seq_prior_3 = Window.orderBy(F.col("event_at_seq").desc(), "id").rowsBetween(-3, -1)
w_all_by_event_at_seq = Window.orderBy(F.col("event_at_seq").desc(), "id")
w_first_event_at_to_date_event = Window.partitionBy(F.to_date("first_event_at"), "event_at_seq")

base = (
    df_ticket_events
    .filter((F.col("actor") == "system") & (F.col("event_at") >= F.lit("2025-01-31 09:00:00").cast("timestamp")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws(" / ", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
    )
)

windowed = (
    base
    .select(
        "*",
        F.lead("event_at").over(w_ticket_id_by_event_at).alias("next_event_at"),
        F.round(F.percent_rank().over(w_all_by_event_at), 3).alias("event_at_percent_rank"),
        F.first("event_at").over(w_all_by_event_at_full).alias("first_event_at"),
        F.row_number().over(w_all_by_event_at).alias("event_at_seq"),
        F.lag("event_at").over(w_event_type_ticket_id_by_even).alias("prev_event_at"),
        F.last("event_at").over(w_all_by_event_at_full).alias("last_event_at"),
        F.last("event_at", ignorenulls=True).over(w_ticket_id_by_event_at_runnin).alias("last_known_event_at"),
        F.first("event_at", ignorenulls=True).over(w_all_by_event_at_running).alias("first_known_event_at"),
        F.dense_rank().over(w_event_type_ticket_id_by_even).alias("event_at_dense_rank"),
        F.nth_value("event_at", 2).over(w_event_type_ticket_id_by_even_2).alias("second_event_at"),
        F.lag("event_at", 3).over(w_event_type_ticket_id_by_even).alias("prev3_event_at"),
        F.round(F.percent_rank().over(w_ticket_id_by_event_at), 3).alias("event_at_percent_rank_2"),
    )
)

leaders = (
    windowed
    .filter((F.col("event_at_dense_rank") <= 5) & (F.col("prev3_event_at") >= F.lit("2025-01-25 10:00:00").cast("timestamp")))
    .drop("event_at_dense_rank")
)

metrics = (
    leaders
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_event_at_seq_ties), 3).alias("event_at_seq_cume_dist"),
        F.round(F.avg("event_at_percent_rank").over(w_all_by_event_at_seq_prior_3), 2).alias("event_at_percent_rank_avg_3_prior"),
        F.rank().over(w_all_by_event_at_seq).alias("event_at_seq_rank"),
        F.round(F.percent_rank().over(w_all_by_event_at_seq_ties), 3).alias("event_at_seq_percent_rank"),
        F.min("event_at_percent_rank_2").over(w_first_event_at_to_date_event).alias("first_event_at_to_date_event_at_seq_min_event_at"),
        F.size(F.collect_set("event_type").over(w_first_event_at_to_date_event)).alias("distinct_event_type_in_first_event_at_to_date_ev"),
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
        "next_event_at",
        "event_at_percent_rank",
        "first_event_at",
        "event_at_seq",
        "last_known_event_at",
        "second_event_at",
        "event_at_percent_rank_2",
        "event_at_seq_cume_dist",
        "event_at_percent_rank_avg_3_prior",
        "event_at_seq_percent_rank",
        "first_event_at_to_date_event_at_seq_min_event_at",
        "distinct_event_type_in_first_event_at_to_date_ev",
        (F.col("event_at_seq_cume_dist") - F.col("event_at_percent_rank")).alias("event_at_seq_cume_dist_minus_event_at_percent_ra"),
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
    )
    .orderBy(
        F.col("event_at_seq_percent_rank").desc_nulls_first(),
        F.col("first_event_at").desc_nulls_first(),
        "id",
    )
    .limit(20)
)
