from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("actor").orderBy(F.col("days_since_event_at_2").desc(), F.col("id").desc())
w_first_event_at_2_month = Window.partitionBy(F.month("first_event_at_2"))
w_actor_by_first_event_at_3_ru = Window.partitionBy("actor").orderBy(F.col("first_event_at_3").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_days_since_event_at_2 = Window.orderBy("days_since_event_at_2", "id")
w_first_event_at_to_date = Window.partitionBy(F.to_date("first_event_at"))
w_event_event_type_count_by_ev = Window.partitionBy("event_event_type_count").orderBy(F.col("event_actor_count_vs_first_event_at_2_month_avg").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared = (
    df_ticket_events
    .filter((F.col("actor").isNotNull()) | (F.col("ticket_id") >= 58))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.date_format(F.col("event_at"), "MMM yyyy").alias("event_at_label"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("event_at")).alias("days_since_event_at_2"),
    )
)

screened = (
    prepared.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (~F.col("t.actor").isin("customer", "system")), "left_semi")
)

unique_rows = (
    screened
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.min("event_at").alias("first_event_at_2"),
    )
)

with_stats = (
    unique_rows.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.first_event_at", "t.first_event_at_2")
)

event_stats_2 = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count(F.when(F.col("actor") == "system", 1)).alias("event_actor_count"),
        F.count(F.when(F.col("event_type") == "assigned", 1)).alias("event_event_type_count"),
        F.min("event_at").alias("first_event_at_3"),
    )
)

summarized_rows = (
    with_stats.alias("s")
    .join(event_stats_2.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_actor_count"), F.lit(0)).alias("event_actor_count"),
        F.coalesce(F.col("t.event_event_type_count"), F.lit(0)).alias("event_event_type_count"),
        "t.first_event_at_3",
    )
)

with_windows = (
    summarized_rows
    .withColumn("event_actor_count_vs_first_event_at_2_month_avg", F.round(F.col("event_actor_count") - F.avg("event_actor_count").over(w_first_event_at_2_month), 2))
    .withColumn("first_known_days_since_event_at_2", F.first("days_since_event_at_2", ignorenulls=True).over(w_actor_by_first_event_at_3_ru))
)

metrics = (
    with_windows
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_days_since_event_at_2), 3).alias("days_since_event_at_2_percent_rank"),
        F.min("days_since_event_at_2").over(w_first_event_at_to_date).alias("first_event_at_to_date_min_days_since_event_at_2"),
        F.lag("days_since_event_at_2").over(w_all_by_days_since_event_at_2).alias("prev_days_since_event_at_2"),
        F.last("event_at").over(w_event_event_type_count_by_ev).alias("last_event_at"),
    )
)

result = (
    metrics
    .filter(F.col("days_since_event_at_2_percent_rank") > 0.1)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "days_since_event_at",
        "event_at_label",
        "days_since_event_at_2",
        "first_event_at",
        "event_actor_count",
        "event_event_type_count",
        "first_event_at_3",
        "event_actor_count_vs_first_event_at_2_month_avg",
        "first_known_days_since_event_at_2",
        "first_event_at_to_date_min_days_since_event_at_2",
        "prev_days_since_event_at_2",
        "last_event_at",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("first_event_at_2")).alias("days_since_first_event_at_2"),
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
    )
    .orderBy(F.col("event_actor_count").desc())
)
