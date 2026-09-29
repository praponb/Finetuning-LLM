from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_by_event_at_ties = Window.partitionBy("event_type").orderBy("event_at")
w_event_type_actor_by_event_at = Window.partitionBy("event_type", "actor").orderBy(F.col("event_at").desc(), "id")
w_event_type_actor_by_event_at_2 = Window.partitionBy("event_type", "actor").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_ticket_events
    .filter(F.to_date(F.col("event_at")) < F.lit("2025-02-24").cast("date"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat(F.col("actor"), F.lit("#"), F.col("id").cast("string")).alias("actor_ref"),
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
        F.date_format(F.col("event_at"), "MMM yyyy").alias("event_at_label"),
    )
)

sequenced = (
    valid
    .select(
        "*",
        F.round(F.percent_rank().over(w_event_type_by_event_at_ties), 3).alias("event_at_percent_rank"),
        F.lag("event_at").over(w_event_type_actor_by_event_at).alias("prev_event_at"),
        F.ntile(3).over(w_event_type_actor_by_event_at).alias("event_at_tercile"),
        F.last("event_at", ignorenulls=True).over(w_event_type_actor_by_event_at_2).alias("last_known_event_at"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_event_type_actor_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
    )
)

exploded_detail = (
    sequenced.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        "s.event_type_or_unknown",
        "s.event_at_label",
        "s.prev_event_at",
        "s.last_known_event_at",
        "s.minutes_since_prev_event_at",
        F.col("t.id").alias("event_id"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_at").alias("event_event_at"),
    )
)

totals = (
    exploded_detail
    .filter(F.col("event_event_at") >= F.lit("2025-01-25 13:00:00").cast("timestamp"))
    .groupBy(
        "event_type_or_unknown",
        F.date_trunc("month", "event_event_at").alias("event_event_at_month"),
    )
    .agg(
        F.sum("minutes_since_prev_event_at").alias("total_minutes_since_prev_event_at"),
        F.sum(F.when(F.col("actor").isNotNull(), F.col("minutes_since_prev_event_at")).otherwise(0)).alias("minutes_since_prev_event_at_actor_is_not_null"),
        F.count(F.when(~F.col("event_event_type").isin("created", "assigned"), 1)).alias("n_event_event_type_not_in_crea"),
        F.percentile_approx("minutes_since_prev_event_at", 0.5).alias("median_minutes_since_prev_event_at"),
        F.count("*").alias("cnt"),
    )
)

with_activity = (
    totals
    .crossJoin(totals.agg(F.avg("total_minutes_since_prev_event_at").alias("overall_avg_total_minutes_since_prev_event_at")))
    .filter(F.col("total_minutes_since_prev_event_at") > F.col("overall_avg_total_minutes_since_prev_event_at"))
    .drop("overall_avg_total_minutes_since_prev_event_at")
)

result = (
    with_activity
    .filter(F.col("n_event_event_type_not_in_crea") <= 6)
    .select("minutes_since_prev_event_at_actor_is_not_null", "cnt")
    .distinct()
)
