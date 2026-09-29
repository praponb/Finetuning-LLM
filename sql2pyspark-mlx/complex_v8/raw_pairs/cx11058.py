from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_running = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type_or_unknown_by_eve = Window.partitionBy("event_type_or_unknown").orderBy(F.col("event_at").desc(), "id")
w_all_by_event_at = Window.orderBy(F.col("event_at").desc(), "id")

valid = (
    df_ticket_events
    .filter(F.col("event_type") != "resolved")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.trim(F.col("event_type"))).alias("event_type_clean"),
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
    )
)

with_trends = (
    valid
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "event_type_clean",
        "event_type_or_unknown",
        F.first("event_at", ignorenulls=True).over(w_all_by_event_at_running).alias("first_known_event_at"),
        F.ntile(5).over(w_event_type_or_unknown_by_eve).alias("event_at_quintile"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_all_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
    )
)

stats = (
    with_trends
    .filter(F.length(F.trim(F.col("event_type_clean"))) >= 4)
    .groupBy("event_at_quintile")
    .agg(
        F.sum("event_at_quintile").alias("total_event_at_quintile"),
        F.round(F.sum("event_at_quintile") / F.nullif(F.sum("minutes_since_prev_event_at"), F.lit(0)), 3).alias("event_at_quintile_to_minutes_since_prev_event_at"),
        F.sum("minutes_since_prev_event_at").alias("total_minutes_since_prev_event_at"),
        F.round(F.stddev("minutes_since_prev_event_at"), 2).alias("stddev_minutes_since_prev_event_at"),
    )
)

result = (
    stats
    .select(
        "event_at_quintile",
        F.col("stddev_minutes_since_prev_event_at").alias("stddev_minutes_since_prev_event_at_v"),
        F.col("total_minutes_since_prev_event_at").alias("total_minutes_since_prev_event_at_v"),
        F.col("event_at_quintile_to_minutes_since_prev_event_at").alias("m_event_at_quintile_to_minutes_since_prev_event_"),
    )
    .unpivot(
        ids=["event_at_quintile"],
        values=["stddev_minutes_since_prev_event_at_v", "total_minutes_since_prev_event_at_v", "m_event_at_quintile_to_minutes_since_prev_event_"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
