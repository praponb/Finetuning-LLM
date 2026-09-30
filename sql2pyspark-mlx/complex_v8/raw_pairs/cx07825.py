from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_by_event_at_runni = Window.partitionBy("event_type").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_at_month_by_event_at = Window.partitionBy(F.date_trunc("month", "event_at")).orderBy("event_at", "id")
w_event_at_to_date_event_type = Window.partitionBy(F.to_date("event_at"), "event_type").orderBy("event_at", "id")
w_event_at_month_by_event_at_r = Window.partitionBy(F.date_trunc("month", "event_at")).orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_actor_by_event_at = Window.partitionBy("ticket_id", "actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_actor_by_event_at_2 = Window.partitionBy("ticket_id", "actor").orderBy("event_at", "id")
w_event_at_to_date_by_event_at = Window.partitionBy(F.to_date("event_at")).orderBy(F.col("event_at").desc(), "id")
w_event_type_by_event_at = Window.partitionBy("event_type").orderBy(F.col("event_at").desc(), "id")
w_ticket_id_actor_by_event_at_3 = Window.partitionBy("ticket_id", "actor").orderBy("event_at")
w_event_at_month_by_event_at_t = Window.partitionBy(F.date_trunc("month", "event_at")).orderBy("event_at")
w_rn_2_by_event_at_tercile_run = Window.partitionBy("rn_2").orderBy(F.col("event_at_tercile").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type = Window.partitionBy("event_type")
w_all_by_first_known_event_at = Window.orderBy("first_known_event_at", "id")
w_all_by_first_known_event_at_2 = Window.orderBy("first_known_event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_first_known_event_at_3 = Window.orderBy("first_known_event_at")

base = (
    df_ticket_events
    .filter(F.to_date(F.col("event_at")) < F.lit("2025-03-08").cast("date"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat(F.col("event_type"), F.lit("#"), F.col("ticket_id").cast("string")).alias("event_type_ref"),
    )
)

metrics = (
    base
    .select(
        "*",
        F.last("event_at", ignorenulls=True).over(w_event_type_by_event_at_runni).alias("last_known_event_at"),
        F.row_number().over(w_event_at_month_by_event_at).alias("rn"),
        F.lag("event_at", 2).over(w_event_at_to_date_event_type).alias("prev2_event_at"),
        F.last("event_at", ignorenulls=True).over(w_event_at_month_by_event_at_r).alias("last_known_event_at_2"),
        F.dense_rank().over(w_event_at_to_date_event_type).alias("event_at_dense_rank"),
        F.first("event_at", ignorenulls=True).over(w_ticket_id_actor_by_event_at).alias("first_known_event_at"),
        F.round(F.percent_rank().over(w_ticket_id_actor_by_event_at_2), 3).alias("event_at_percent_rank"),
        F.lead("event_at", 3).over(w_event_at_to_date_event_type).alias("next3_event_at"),
        F.ntile(3).over(w_event_at_to_date_by_event_at).alias("event_at_tercile"),
        F.round(F.cume_dist().over(w_event_type_by_event_at), 3).alias("event_at_cume_dist"),
        F.row_number().over(w_ticket_id_actor_by_event_at_2).alias("rn_2"),
        F.round(F.cume_dist().over(w_ticket_id_actor_by_event_at_3), 3).alias("event_at_cume_dist_2"),
        F.round(F.percent_rank().over(w_event_at_month_by_event_at_t), 3).alias("event_at_percent_rank_2"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_event_type_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
    )
)

windowed = (
    metrics
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        "event_type_ref",
        "last_known_event_at",
        "rn",
        "last_known_event_at_2",
        "event_at_dense_rank",
        "first_known_event_at",
        "event_at_percent_rank",
        "next3_event_at",
        "rn_2",
        "event_at_percent_rank_2",
        "minutes_since_prev_event_at",
        F.last("event_at_tercile", ignorenulls=True).over(w_rn_2_by_event_at_tercile_run).alias("last_known_event_at_tercile"),
        F.sum("event_at_percent_rank").over(w_event_type).alias("event_type_sum_event_at_percent_rank"),
        F.lead("rn_2", 2).over(w_all_by_first_known_event_at).alias("next2_rn_2"),
        F.size(F.collect_set("ticket_id").over(w_event_type)).alias("distinct_ticket_id_in_event_type"),
        F.first("rn_2", ignorenulls=True).over(w_all_by_first_known_event_at_2).alias("first_known_rn_2"),
        F.round(F.cume_dist().over(w_all_by_first_known_event_at_3), 3).alias("first_known_event_at_cume_dist"),
        F.sum("event_at_dense_rank").over(w_event_type).alias("event_type_sum_event_at_dense_rank"),
    )
)

result = (
    windowed
    .select(
        "id",
        "event_at",
        "actor",
        "last_known_event_at",
        "rn",
        "last_known_event_at_2",
        "first_known_event_at",
        "event_at_percent_rank",
        "next3_event_at",
        "rn_2",
        "event_at_percent_rank_2",
        "minutes_since_prev_event_at",
        "last_known_event_at_tercile",
        "next2_rn_2",
        "distinct_ticket_id_in_event_type",
        "first_known_rn_2",
        "first_known_event_at_cume_dist",
        "event_type_sum_event_at_dense_rank",
        F.concat_ws("-", F.col("event_type_ref"), F.col("event_type")).alias("event_type_ref_event_type_label"),
    )
)
