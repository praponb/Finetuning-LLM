from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_len_actor_by_even = Window.partitionBy("event_type_len", "actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type_by_event_at_movin = Window.partitionBy("event_type").orderBy("event_at", "id").rowsBetween(-1, Window.currentRow)
w_ticket_id = Window.partitionBy("ticket_id")
w_all_by_event_type_len_prior = Window.orderBy("event_type_len", "id").rowsBetween(-1, -1)
w_event_at_to_date = Window.partitionBy(F.to_date("event_at"))
w_all_by_event_at_running = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_event_type_len_by = Window.partitionBy("ticket_id", "event_type_len").orderBy(F.col("event_type_len").desc(), "id")
w_all_by_first_known_event_at = Window.orderBy("first_known_event_at", "id")
w_actor_event_type_len_by_firs = Window.partitionBy("actor", "event_type_len").orderBy("first_known_event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_month_actor = Window.partitionBy(F.date_trunc("month", "event_at"), "actor")

valid = (
    df_ticket_events
    .filter(F.col("ticket_id") >= 31)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.substring(F.trim(F.col("event_type")), 1, 2).alias("event_type_prefix"),
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
    )
)

with_trends = (
    valid
    .select(
        "id",
        "event_at",
        "actor",
        "event_type_len",
        F.max("event_type_len").over(w_event_type_len_actor_by_even).alias("max_event_type_len_so_far"),
        F.sum("event_type_len").over(w_event_type_by_event_at_movin).alias("event_type_len_sum_2"),
        F.round(F.col("event_type_len") / F.nullif(F.sum("event_type_len").over(w_ticket_id), F.lit(0)), 4).alias("event_type_len_share_of_ticket_id"),
        F.max("event_type_len").over(w_all_by_event_type_len_prior).alias("event_type_len_max_1_prior"),
        F.max("event_type_len").over(w_event_at_to_date).alias("event_at_to_date_max_event_type_len"),
        F.first("event_at", ignorenulls=True).over(w_all_by_event_at_running).alias("first_known_event_at"),
        F.row_number().over(w_ticket_id_event_type_len_by).alias("seq_no"),
    )
)

top_rows = (
    with_trends
    .filter(F.col("seq_no") <= 5)
    .drop("seq_no")
)

ranked = (
    top_rows
    .select(
        "id",
        "event_at",
        "event_type_len",
        "event_type_len_share_of_ticket_id",
        "event_type_len_max_1_prior",
        "first_known_event_at",
        F.row_number().over(w_all_by_first_known_event_at).alias("first_known_event_at_seq"),
        F.nth_value("max_event_type_len_so_far", 2).over(w_actor_event_type_len_by_firs).alias("second_max_event_type_len_so_far"),
        F.round(F.col("event_type_len_sum_2") - F.avg("event_type_len_sum_2").over(w_event_at_month_actor), 2).alias("event_type_len_sum_2_vs_event_at_month_actor_avg"),
        F.round(F.col("event_type_len") / F.nullif(F.sum("event_type_len").over(w_event_at_month_actor), F.lit(0)), 4).alias("event_type_len_share_of_event_at_month_actor"),
        F.count(F.lit(1)).over(w_event_at_month_actor).alias("rows_in_event_at_month_actor"),
        F.sum("max_event_type_len_so_far").over(w_event_at_month_actor).alias("event_at_month_actor_sum_max_event_type_len_so_f"),
    )
)

result = (
    ranked
    .filter(F.col("rows_in_event_at_month_actor") >= 9)
    .select(
        "id",
        "event_at",
        "event_type_len",
        "event_type_len_share_of_ticket_id",
        "event_type_len_max_1_prior",
        "first_known_event_at",
        "first_known_event_at_seq",
        "second_max_event_type_len_so_far",
        "rows_in_event_at_month_actor",
        "event_at_month_actor_sum_max_event_type_len_so_f",
        (F.col("event_at") + F.expr("INTERVAL 2 HOURS")).alias("event_at_plus_2h"),
    )
    .orderBy("id", "second_max_event_type_len_so_far")
)
