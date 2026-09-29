from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type = Window.partitionBy("event_type").orderBy("event_at", "id")
w_event_type_running = Window.partitionBy("event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_ticket_events
    .filter(F.col("event_at") < F.lit("2025-03-02 10:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_prev = cleaned.withColumn("prev_event_at", F.lag("event_at").over(w_event_type))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_event_type_running))

rollup_base = (
    sessionized
    .groupBy(
        F.date_trunc("month", "event_at").alias("event_at_month"),
        "burst_id",
        "is_new_session",
    )
    .agg(
        F.max("burst_id").alias("max_burst_id"),
        F.array_sort(F.collect_set("event_type")).alias("event_type_list"),
        F.count("*").alias("row_count"),
        F.max("event_at").alias("last_event_at"),
    )
)

scored = (
    rollup_base
    .filter(F.to_date(F.col("last_event_at")) < F.lit("2025-02-12").cast("date"))
    .select(
        "*",
        F.quarter(F.col("last_event_at")).alias("last_event_at_quarter"),
        F.greatest(F.col("max_burst_id"), F.col("is_new_session")).alias("greatest_max_burst_id_is_new_session"),
    )
)

result = (
    scored
    .select(
        "event_at_month",
        "is_new_session",
        "max_burst_id",
        "event_type_list",
        "row_count",
        "last_event_at",
        "last_event_at_quarter",
        "greatest_max_burst_id_is_new_session",
    )
)
