from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_event_at_runnin = Window.orderBy(F.col("event_event_at").desc(), "id", "event_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_n_event_type_resolved_event = Window.partitionBy("n_event_type_resolved", F.date_trunc("month", "event_event_at_month"))

cleaned = (
    df_ticket_events
    .filter(F.col("event_type").isin("resolved", "created"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.substring(F.trim(F.col("event_type")), 1, 4).alias("event_type_prefix"),
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
    )
)

with_children_events = (
    cleaned.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_at") >= F.lit("2025-01-31 08:00:00").cast("timestamp")), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.event_type_prefix",
        "s.event_type_slug",
        F.col("t.id").alias("event_id"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.actor").alias("event_actor"),
    )
)

filtered = (
    with_children_events.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.closed_at") >= F.lit("2025-02-04 23:00:00").cast("timestamp")), "left_semi")
)

sequenced = (
    filtered
    .select(
        "id",
        "ticket_id",
        "event_type",
        "event_type_prefix",
        "event_id",
        "event_event_at",
        F.first("event_event_at", ignorenulls=True).over(w_all_by_event_event_at_runnin).alias("first_known_event_event_at"),
        F.last("event_event_at", ignorenulls=True).over(w_all_by_event_event_at_runnin).alias("last_known_event_event_at"),
    )
)

rollup_base = (
    sequenced
    .groupBy(
        "event_id",
        F.date_trunc("month", "event_event_at").alias("event_event_at_month"),
    )
    .agg(
        F.count(F.when(F.col("event_type") == "escalated", 1)).alias("n_event_type_escalated"),
        F.count(F.when(F.col("event_type").isin("reopened", "created"), 1)).alias("n_event_type_in_reopened_creat"),
        F.max("last_known_event_event_at").alias("last_last_known_event_event_at"),
        F.sum(F.when(F.col("event_type") != "resolved", 1).otherwise(0)).alias("n_event_type_resolved"),
    )
)

enriched = (
    rollup_base
    .select(
        "*",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("event_event_at_month")).alias("days_since_event_event_at_month"),
        F.greatest(F.col("n_event_type_resolved"), F.col("n_event_type_escalated")).alias("greatest_n_event_type_resolved_n_event_type_esca"),
        F.least(F.col("n_event_type_resolved"), F.col("n_event_type_escalated")).alias("least_n_event_type_resolved_n_event_type_escalat"),
    )
)

with_windows = (
    enriched
    .withColumn("rows_in_n_event_type_resolved_event_event_at_mon", F.count(F.lit(1)).over(w_n_event_type_resolved_event))
    .withColumn("n_event_type_escalated_share_of_n_event_type_res", F.round(F.col("n_event_type_escalated") / F.nullif(F.sum("n_event_type_escalated").over(w_n_event_type_resolved_event), F.lit(0)), 4))
)

result = (
    with_windows
    .filter(F.col("rows_in_n_event_type_resolved_event_event_at_mon") > 8)
    .select(
        "event_id",
        "n_event_type_escalated",
        "n_event_type_in_reopened_creat",
        "n_event_type_resolved",
        "days_since_event_event_at_month",
        "greatest_n_event_type_resolved_n_event_type_esca",
        "least_n_event_type_resolved_n_event_type_escalat",
        "rows_in_n_event_type_resolved_event_event_at_mon",
        "n_event_type_escalated_share_of_n_event_type_res",
        F.date_trunc("hour", F.col("event_event_at_month")).alias("event_event_at_month_hour"),
    )
    .orderBy(
        F.col("least_n_event_type_resolved_n_event_type_escalat").desc_nulls_first(),
    )
)
