from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_event_type_by_event_at = Window.partitionBy("actor", "event_type").orderBy("event_at_quarter", "id")
w_actor_event_type_by_event_at_2 = Window.partitionBy("actor", "event_type").orderBy("event_at_quarter", "id").rowsBetween(-3, Window.currentRow)
w_event_type = Window.partitionBy("event_type")
w_event_at_quarter_by_event_at = Window.partitionBy("event_at_quarter").orderBy(F.col("event_at_quarter").desc(), "id")
w_event_type_ticket_id_by_even = Window.partitionBy("event_type", "ticket_id").orderBy(F.col("event_at_quarter").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_event_type = Window.partitionBy("ticket_id", "event_type")
w_all_by_event_at_moving_1 = Window.orderBy("event_at", "id").rowsBetween(-1, Window.currentRow)
w_actor_event_at_quarter_quart = Window.partitionBy("actor", "event_at_quarter_quartile").orderBy("event_at_quarter_quartile", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_event_at_quarter_quart_2 = Window.partitionBy("actor", "event_at_quarter_quartile").orderBy("event_at_quarter_quartile", "id").rowsBetween(-3, Window.currentRow)
w_ticket_id_event_at_month = Window.partitionBy("ticket_id", F.date_trunc("month", "event_at"))
w_actor_event_at_quarter_quart_3 = Window.partitionBy("actor", "event_at_quarter_quartile").orderBy("event_at_quarter_quartile", "id").rowsBetween(-2, -1)
w_all_by_event_type_min_event = Window.orderBy("event_type_min_event_at_quarter", "id")
w_event_at_month_by_event_at_q = Window.partitionBy(F.date_trunc("month", "event_at")).orderBy("event_at_quarter_avg_4", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned = (
    df_ticket_events
    .filter((F.to_date(F.col("event_at")) < F.lit("2025-02-12").cast("date")) & (F.col("ticket_id").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "yyyy-MM-dd HH:00").alias("event_at_label"),
        F.quarter(F.col("event_at")).alias("event_at_quarter"),
    )
)

metrics = (
    cleaned
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "event_at_label",
        F.lag("event_at_quarter", 2, 0).over(w_actor_event_type_by_event_at).alias("prev2_event_at_quarter"),
        F.round(F.avg("event_at_quarter").over(w_actor_event_type_by_event_at_2), 2).alias("event_at_quarter_avg_4"),
        F.min("event_at_quarter").over(w_event_type).alias("event_type_min_event_at_quarter"),
        F.ntile(3).over(w_event_at_quarter_by_event_at).alias("event_at_quarter_tercile"),
        F.ntile(4).over(w_event_at_quarter_by_event_at).alias("event_at_quarter_quartile"),
        F.count(F.lit(1)).over(w_event_type_ticket_id_by_even).alias("running_count"),
        F.round(F.avg("event_at_quarter").over(w_event_type), 2).alias("event_type_avg_event_at_quarter"),
        F.size(F.collect_set("actor").over(w_ticket_id_event_type)).alias("distinct_actor_in_ticket_id_event_type"),
        F.round(F.avg("event_at_quarter").over(w_all_by_event_at_moving_1), 2).alias("event_at_quarter_avg_2"),
    )
)

with_trends = (
    metrics
    .select(
        "*",
        F.nth_value("event_at_quarter_tercile", 2).over(w_actor_event_at_quarter_quart).alias("second_event_at_quarter_tercile"),
        F.round(F.avg("event_at_quarter_tercile").over(w_actor_event_at_quarter_quart_2), 2).alias("event_at_quarter_tercile_avg_4"),
        F.max("running_count").over(w_ticket_id_event_at_month).alias("ticket_id_event_at_month_max_running_count"),
        F.round(F.avg("prev2_event_at_quarter").over(w_ticket_id_event_at_month), 2).alias("ticket_id_event_at_month_avg_prev2_event_at_quar"),
        F.max("event_at_quarter_quartile").over(w_ticket_id_event_at_month).alias("ticket_id_event_at_month_max_event_at_quarter_qu"),
        F.sum("event_at_quarter_avg_4").over(w_actor_event_at_quarter_quart_3).alias("event_at_quarter_avg_4_sum_2_prior"),
        F.row_number().over(w_all_by_event_type_min_event).alias("seq_no"),
        F.last("prev2_event_at_quarter").over(w_event_at_month_by_event_at_q).alias("last_prev2_event_at_quarter"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "event_at",
        "event_type",
        "event_at_label",
        "event_type_min_event_at_quarter",
        "event_at_quarter_tercile",
        "event_type_avg_event_at_quarter",
        "distinct_actor_in_ticket_id_event_type",
        "event_at_quarter_avg_2",
        "event_at_quarter_tercile_avg_4",
        "ticket_id_event_at_month_max_running_count",
        "ticket_id_event_at_month_avg_prev2_event_at_quar",
        "ticket_id_event_at_month_max_event_at_quarter_qu",
        "event_at_quarter_avg_4_sum_2_prior",
        F.date_trunc("quarter", F.col("event_at")).alias("event_at_quarter"),
    )
    .orderBy("event_at_quarter_avg_2")
)
