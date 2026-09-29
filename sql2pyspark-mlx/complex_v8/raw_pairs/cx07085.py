from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_by_event_at_ties = Window.partitionBy("event_type").orderBy(F.col("event_at").desc())
w_event_type_by_event_at = Window.partitionBy("event_type").orderBy(F.col("event_at").desc(), "id")
w_event_type_by_event_at_full = Window.partitionBy("event_type").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid = (
    df_ticket_events
    .filter((F.col("ticket_id") <= 24) | (F.col("event_at") >= F.lit("2025-01-25 07:00:00").cast("timestamp")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
        F.concat(F.col("event_type"), F.lit("#"), F.col("ticket_id").cast("string")).alias("event_type_ref"),
        F.substring(F.trim(F.col("event_type")), 1, 4).alias("event_type_prefix"),
    )
)

sequenced = (
    valid
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "event_type_slug",
        "event_type_ref",
        F.round(F.cume_dist().over(w_event_type_by_event_at_ties), 3).alias("event_at_cume_dist"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_event_type_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.lag("event_at", 2).over(w_event_type_by_event_at).alias("prev2_event_at"),
        F.last("event_at").over(w_event_type_by_event_at_full).alias("last_event_at"),
    )
)

result = (
    sequenced
    .select("id", "event_at_cume_dist", "minutes_since_prev_event_at")
    .unpivot(
        ids=["id"],
        values=["event_at_cume_dist", "minutes_since_prev_event_at"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "metric")
)
