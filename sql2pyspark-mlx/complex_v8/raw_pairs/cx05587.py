from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("ticket_id", "event_type").orderBy(F.col("event_at").desc(), F.col("id").desc())
w_event_at_month_by_event_at = Window.partitionBy(F.month("event_at")).orderBy(F.col("event_at").desc(), "id")
w_event_at_month_by_event_at_r = Window.partitionBy(F.month("event_at")).orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_at_month_by_event_at_t = Window.partitionBy(F.month("event_at")).orderBy(F.col("event_at").desc())

recent_events = (
    df_ticket_events
    .filter((F.col("actor").isNotNull()) & (F.col("event_at") >= F.lit("2025-02-06 11:00:00").cast("timestamp")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.substring(F.trim(F.col("actor")), 1, 4).alias("actor_prefix"),
    )
)

deduped = (
    recent_events
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

with_activity = (
    deduped.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left_semi")
    .filter(F.col("event_type") == "created")
)

sequenced = (
    with_activity
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "actor_prefix",
        F.lag("event_at", 3).over(w_event_at_month_by_event_at).alias("prev3_event_at"),
        F.ntile(10).over(w_event_at_month_by_event_at).alias("event_at_decile"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_event_at_month_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.last("event_at", ignorenulls=True).over(w_event_at_month_by_event_at_r).alias("last_known_event_at"),
        F.row_number().over(w_event_at_month_by_event_at).alias("rn"),
        F.round(F.cume_dist().over(w_event_at_month_by_event_at_t), 3).alias("event_at_cume_dist"),
    )
)

derived = (
    sequenced
    .filter(F.col("event_type") == "created")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "actor_prefix",
        "event_at_decile",
        "minutes_since_prev_event_at",
        "last_known_event_at",
        "rn",
        "event_at_cume_dist",
        (F.col("event_at_decile") - F.col("rn")).alias("event_at_decile_minus_rn"),
        F.date_trunc("year", F.col("last_known_event_at")).alias("last_known_event_at_year"),
        (F.col("event_at") + F.expr("INTERVAL 15 MINUTES")).alias("event_at_plus_15m"),
    )
)

with_children_events = (
    derived.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor_prefix",
        "s.minutes_since_prev_event_at",
        "s.event_at_decile_minus_rn",
        F.col("t.id").alias("event_id"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.actor").alias("event_actor"),
    )
)

result = (
    with_children_events
    .select(
        "id",
        "ticket_id",
        "actor_prefix",
        "minutes_since_prev_event_at",
        "event_at_decile_minus_rn",
        "event_id",
        "event_event_type",
        "event_event_at",
        "event_actor",
        F.concat_ws(" / ", F.col("event_actor"), F.col("event_event_type")).alias("event_actor_event_event_type_label"),
    )
    .orderBy(F.col("event_event_at").desc(), "event_event_type")
)
