from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_at_month_event_type_by = Window.partitionBy(F.month("event_at"), "event_type").orderBy("event_at", "id")
w_event_at_month_event_type_by_2 = Window.partitionBy(F.month("event_at"), "event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_month_event_type_by_3 = Window.partitionBy(F.month("event_at"), "event_type").orderBy("event_at")
w_dedup = Window.partitionBy("event_type").orderBy(F.col("event_at").desc(), F.col("id").desc(), F.col("event_id").desc())

base = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

metrics = (
    base
    .select(
        "*",
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_event_at_month_event_type_by))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.nth_value("event_at", 2).over(w_event_at_month_event_type_by_2).alias("second_event_at"),
        F.round(F.cume_dist().over(w_event_at_month_event_type_by), 3).alias("event_at_cume_dist"),
        F.lead("event_at", 2).over(w_event_at_month_event_type_by).alias("next2_event_at"),
        F.round(F.percent_rank().over(w_event_at_month_event_type_by_3), 3).alias("event_at_percent_rank"),
    )
)

filtered = (
    metrics.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.actor").isNotNull()) & (F.to_date(F.col("t.event_at")) < F.lit("2025-02-24").cast("date")), "left_semi")
)

exploded_detail_events = (
    filtered.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.minutes_since_prev_event_at",
        "s.second_event_at",
        "s.event_at_percent_rank",
        F.col("t.id").alias("event_id"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_at").alias("event_event_at"),
    )
)

unique_rows = (
    exploded_detail_events
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    unique_rows
    .groupBy("event_event_at")
    .pivot("actor", ["system", "customer", "agent"])
    .agg(F.max("minutes_since_prev_event_at"))
    .orderBy("event_event_at")
)
