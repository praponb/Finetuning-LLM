from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_by_event_at = Window.partitionBy("event_type").orderBy("event_at", "id")
w_event_type_by_event_at_ties = Window.partitionBy("event_type").orderBy("event_at")
w_dedup = Window.partitionBy("actor").orderBy(F.col("prev3_event_at").desc(), F.col("id").desc())

cleaned = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

qualified = (
    cleaned.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.actor").isin("agent", "customer")) & (F.col("t.event_at") < F.lit("2025-03-08 09:00:00").cast("timestamp")), "left_anti")
)

with_trends = (
    qualified
    .select(
        "id",
        "event_at",
        "actor",
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_event_type_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.round(F.cume_dist().over(w_event_type_by_event_at_ties), 3).alias("event_at_cume_dist"),
        F.lag("event_at", 3).over(w_event_type_by_event_at).alias("prev3_event_at"),
    )
)

unique_rows = (
    with_trends
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

annotated = (
    unique_rows
    .filter(F.col("event_at") >= F.lit("2025-01-25 10:00:00").cast("timestamp"))
    .select(
        "id",
        "event_at",
        "actor",
        "minutes_since_prev_event_at",
        F.date_trunc("quarter", F.col("prev3_event_at")).alias("prev3_event_at_quarter"),
        F.dayofweek(F.col("event_at")).alias("event_at_dayofweek"),
    )
)

summary = (
    annotated
    .groupBy(
        "actor",
        F.date_trunc("month", "prev3_event_at_quarter").alias("prev3_event_at_quarter_month"),
        F.year("event_at").alias("event_at_year"),
    )
    .agg(
        F.round(F.sum("event_at_dayofweek") / F.nullif(F.sum("minutes_since_prev_event_at"), F.lit(0)), 3).alias("event_at_dayofweek_to_minutes_since_prev_event_a"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") >= 3)
)

scored = summary.withColumn("actor_or_unknown", F.coalesce(F.col("actor"), F.lit("UNKNOWN")))

result = (
    scored
    .select(
        "actor",
        "prev3_event_at_quarter_month",
        "event_at_dayofweek_to_minutes_since_prev_event_a",
        "cnt",
        "actor_or_unknown",
    )
    .orderBy("prev3_event_at_quarter_month")
)
