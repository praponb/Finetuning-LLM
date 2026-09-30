from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_event_at_month_mo = Window.partitionBy("event_type", F.month("event_at_month")).orderBy(F.col("median_hours_since_open").desc(), "event_type", "event_at_month")
w_event_type_event_at_month_mo_2 = Window.partitionBy("event_type", F.month("event_at_month")).orderBy(F.col("median_hours_since_open").desc(), "event_type", "event_at_month").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_first_event_at_month = Window.partitionBy(F.date_trunc("month", "first_event_at"))
w_event_type_event_at_month_to = Window.partitionBy("event_type", F.to_date("event_at_month")).orderBy("first_event_at")

covered = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 1 DAYS"))),
        "inner",
    )
    .select(
        "e.id",
        "e.ticket_id",
        "e.event_at",
        "e.event_type",
        "t.priority",
        "t.agent_id",
        F.round((F.unix_timestamp("e.event_at") - F.unix_timestamp("t.opened_at")) / 3600, 2).alias("hours_since_open"),
    )
)

rollup_base = (
    covered
    .groupBy("event_type", F.date_trunc("month", "event_at").alias("event_at_month"))
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.min("hours_since_open").alias("min_hours_since_open"),
        F.percentile_approx("hours_since_open", 0.5).alias("median_hours_since_open"),
    )
)

ranked = (
    rollup_base
    .select(
        "*",
        F.lag("first_event_at", 2).over(w_event_type_event_at_month_mo).alias("prev2_first_event_at"),
        F.max("min_hours_since_open").over(w_event_type_event_at_month_mo_2).alias("max_min_hours_since_open_so_far"),
        F.round(F.col("min_hours_since_open") - F.avg("min_hours_since_open").over(w_first_event_at_month), 2).alias("min_hours_since_open_vs_first_event_at_month_avg"),
        F.count(F.lit(1)).over(w_first_event_at_month).alias("rows_in_first_event_at_month"),
        F.dense_rank().over(w_event_type_event_at_month_to).alias("first_event_at_dense_rank"),
        F.sum("median_hours_since_open").over(w_first_event_at_month).alias("first_event_at_month_sum_median_hours_since_open"),
    )
)

without_matches = (
    ranked
    .crossJoin(ranked.agg(F.max("rows_in_first_event_at_month").alias("overall_max_rows_in_first_event_at_month")))
    .filter(F.col("rows_in_first_event_at_month") < F.col("overall_max_rows_in_first_event_at_month"))
    .drop("overall_max_rows_in_first_event_at_month")
)

result = (
    without_matches
    .select(
        "event_type",
        "event_at_month",
        "median_hours_since_open",
        "prev2_first_event_at",
        "max_min_hours_since_open_so_far",
        "first_event_at_dense_rank",
        "first_event_at_month_sum_median_hours_since_open",
        F.initcap(F.trim(F.col("event_type"))).alias("event_type_clean"),
    )
    .orderBy("event_type")
)
