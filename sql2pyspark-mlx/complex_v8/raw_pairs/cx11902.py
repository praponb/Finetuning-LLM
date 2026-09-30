from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_month = Window.orderBy(F.col("event_at_month").desc(), "id")
w_all_by_event_at = Window.orderBy(F.col("event_at").desc(), "id")
w_event_type_event_at_month_by = Window.partitionBy("event_type", "event_at_month").orderBy(F.col("event_at_month").desc())
w_event_at_month = Window.partitionBy("event_at_month")
w_all_by_event_at_running = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_events = (
    df_ticket_events
    .filter((F.hour(F.col("event_at")).between(10, 13)) & (F.col("ticket_id") <= 18))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.month(F.col("event_at")).alias("event_at_month"),
    )
)

with_trends = (
    scoped_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "event_at_month",
        F.row_number().over(w_all_by_event_at_month).alias("seq_no"),
        F.row_number().over(w_all_by_event_at).alias("event_at_seq"),
        F.round(F.percent_rank().over(w_event_type_event_at_month_by), 3).alias("event_at_month_percent_rank"),
        F.round(F.col("event_at_month") / F.nullif(F.sum("event_at_month").over(w_event_at_month), F.lit(0)), 4).alias("event_at_month_share_of_event_at_month"),
        F.round(F.col("event_at_month") - F.avg("event_at_month").over(w_event_at_month), 2).alias("event_at_month_vs_event_at_month_avg"),
        F.dense_rank().over(w_all_by_event_at).alias("event_at_dense_rank"),
        F.last("event_at", ignorenulls=True).over(w_all_by_event_at_running).alias("last_known_event_at"),
    )
)

totals = (
    with_trends
    .groupBy(
        F.to_date("last_known_event_at").alias("last_known_event_at_day"),
        "event_at_dense_rank",
    )
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.array_sort(F.collect_set("actor")).alias("actor_list"),
        F.round(F.stddev("event_at_month_vs_event_at_month_avg"), 2).alias("stddev_event_at_month_vs_event_at_month_avg"),
    )
)

result = (
    totals
    .select(
        "last_known_event_at_day",
        "event_at_dense_rank",
        "first_event_at",
        "actor_list",
        "stddev_event_at_month_vs_event_at_month_avg",
    )
    .orderBy(
        "last_known_event_at_day",
        F.col("stddev_event_at_month_vs_event_at_month_avg").asc(),
    )
)
