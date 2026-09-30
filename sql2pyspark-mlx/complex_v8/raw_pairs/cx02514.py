from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type = Window.partitionBy("event_type")
w_event_type_by_days_since_eve = Window.partitionBy("event_type").orderBy(F.col("days_since_event_at").asc_nulls_last(), "id")

filtered_events = (
    df_ticket_events
    .filter((F.hour(F.col("event_at")).between(6, 8)) | (F.col("ticket_id") >= 4))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.upper(F.trim(F.col("event_type"))).alias("event_type_clean"),
    )
)

with_trends = (
    filtered_events
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "days_since_event_at",
        "event_type_clean",
        F.round(F.col("days_since_event_at") / F.nullif(F.sum("days_since_event_at").over(w_event_type), F.lit(0)), 4).alias("days_since_event_at_share_of_event_type"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_event_type_by_days_since_eve))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.size(F.collect_set("actor").over(w_event_type)).alias("distinct_actor_in_event_type"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "minutes_since_prev_event_at",
        "days_since_event_at_share_of_event_type",
    )
    .unpivot(
        ids=["id"],
        values=["minutes_since_prev_event_at", "days_since_event_at_share_of_event_type"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
