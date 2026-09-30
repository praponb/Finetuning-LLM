from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_week = Window.orderBy("event_at_week", "id")
w_event_type = Window.partitionBy("event_type")
w_event_type_by_event_at_week = Window.partitionBy("event_type").orderBy(F.col("event_at_week").desc(), "id")
w_ticket_id_event_type = Window.partitionBy("ticket_id", "event_type")
w_ticket_id_by_days_since_even = Window.partitionBy("ticket_id").orderBy(F.col("days_since_event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_by_days_since_even_2 = Window.partitionBy("ticket_id").orderBy(F.col("days_since_event_at").desc(), "id").rowsBetween(-3, -1)
w_event_at_month_event_at_week = Window.partitionBy(F.month("event_at"), "event_at_week_decile").orderBy("hours_event_at_to_event_at_week", "id")
w_event_at_week_year_event_at = Window.partitionBy(F.year("event_at_week"), F.month("event_at"))
w_actor_distinct_actor_in_tick = Window.partitionBy("actor", "distinct_actor_in_ticket_id_event_type")

scoped = (
    df_ticket_events
    .filter((F.col("event_type") == "replied") & (F.col("actor") == "customer"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_trunc("week", F.col("event_at")).alias("event_at_week"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

with_windows = (
    scoped
    .select(
        "id",
        "event_at",
        "actor",
        "event_at_week",
        "days_since_event_at",
        F.ntile(10).over(w_all_by_event_at_week).alias("event_at_week_decile"),
        F.round(F.cume_dist().over(w_all_by_event_at_week), 3).alias("event_at_week_cume_dist"),
        F.min("days_since_event_at").over(w_event_type).alias("event_type_min_days_since_event_at"),
        (F.col("days_since_event_at") - F.lag("days_since_event_at").over(w_event_type_by_event_at_week)).alias("days_since_event_at_change"),
        F.count(F.lit(1)).over(w_event_type).alias("rows_in_event_type"),
        F.size(F.collect_set("actor").over(w_ticket_id_event_type)).alias("distinct_actor_in_ticket_id_event_type"),
        F.max("days_since_event_at").over(w_ticket_id_by_days_since_even).alias("max_days_since_event_at_so_far"),
        F.max("days_since_event_at").over(w_ticket_id_by_days_since_even_2).alias("days_since_event_at_max_3_prior"),
    )
)

annotated = (
    with_windows
    .filter(F.col("event_at") >= F.lit("2025-01-13 14:00:00").cast("timestamp"))
    .select(
        "*",
        F.round((F.unix_timestamp(F.col("event_at_week")) - F.unix_timestamp(F.col("event_at"))) / 3600, 1).alias("hours_event_at_to_event_at_week"),
        F.substring(F.trim(F.col("actor")), 1, 2).alias("actor_prefix"),
        F.round(F.col("event_at_week_cume_dist") * 1.1, 2).alias("event_at_week_cume_dist_adj"),
    )
)

with_windows_2 = (
    annotated
    .select(
        "*",
        F.lead("event_at", 2).over(w_event_at_month_event_at_week).alias("next2_event_at"),
        F.round(F.col("event_at_week_cume_dist") - F.avg("event_at_week_cume_dist").over(w_event_at_week_year_event_at), 2).alias("event_at_week_cume_dist_vs_event_at_week_year_ev"),
        F.count(F.lit(1)).over(w_event_at_week_year_event_at).alias("rows_in_event_at_week_year_event_at_month"),
        F.round(F.col("rows_in_event_type") - F.avg("rows_in_event_type").over(w_actor_distinct_actor_in_tick), 2).alias("rows_in_event_type_vs_actor_distinct_actor_in_ti"),
    )
)

grouped = (
    with_windows_2
    .groupBy(F.date_trunc("month", "event_at").alias("event_at_month"))
    .agg(
        F.round(F.sum("days_since_event_at_max_3_prior") / F.nullif(F.sum("event_at_week_cume_dist"), F.lit(0)), 3).alias("days_since_event_at_max_3_prior_to_event_at_week"),
        F.round(F.sum("days_since_event_at_max_3_prior") / F.nullif(F.sum("max_days_since_event_at_so_far"), F.lit(0)), 3).alias("days_since_event_at_max_3_prior_to_max_days_sinc"),
        F.round(F.stddev("max_days_since_event_at_so_far"), 2).alias("stddev_max_days_since_event_at_so_far"),
        F.array_sort(F.collect_set("actor")).alias("actor_list"),
        F.avg("event_type_min_days_since_event_at").alias("avg_event_type_min_days_since_event_at_check"),
    )
    .filter(F.col("avg_event_type_min_days_since_event_at_check") > 1050)
    .drop("avg_event_type_min_days_since_event_at_check")
)

result = (
    grouped
    .select(
        "event_at_month",
        "days_since_event_at_max_3_prior_to_event_at_week",
        "days_since_event_at_max_3_prior_to_max_days_sinc",
        "stddev_max_days_since_event_at_so_far",
        "actor_list",
        F.date_format(F.col("event_at_month"), "MMM yyyy").alias("event_at_month_label"),
        F.to_date(F.col("event_at_month")).alias("event_at_month_date"),
    )
    .orderBy(F.col("event_at_month_date").asc_nulls_last())
)
