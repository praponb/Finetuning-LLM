from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_actor_by_event_at_runn = Window.partitionBy("event_actor").orderBy("event_at", "id", "event_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_actor_by_event_event_a = Window.partitionBy("event_actor").orderBy(F.col("event_event_at").desc(), "id", "event_id")

filtered_events = (
    df_ticket_events
    .filter(F.col("event_at") >= F.lit("2025-01-13 11:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
        F.date_format(F.col("event_at"), "yyyy-MM").alias("event_at_label"),
    )
)

exploded_detail = (
    filtered_events.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor_or_unknown",
        "s.event_at_label",
        F.col("t.id").alias("event_id"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.actor").alias("event_actor"),
    )
)

with_trends = (
    exploded_detail
    .select(
        "*",
        F.last("event_event_at", ignorenulls=True).over(w_event_actor_by_event_at_runn).alias("last_known_event_event_at"),
        F.ntile(10).over(w_event_actor_by_event_event_a).alias("event_event_at_decile"),
    )
)

result = (
    with_trends
    .groupingSets(
        [["event_actor", "event_event_at_decile"], ["event_actor"], ["event_event_at_decile"], []],
        "event_actor", "event_event_at_decile",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.countDistinct("event_id").alias("distinct_event_ids"),
        F.max("event_event_at_decile").alias("max_event_event_at_decile"),
    )
    .select(
        F.coalesce(F.col("event_actor"), F.lit("ALL")).alias("event_actor"),
        "event_event_at_decile",
        "gid",
        "distinct_event_ids",
        "max_event_event_at_decile",
    )
    .orderBy(
        F.col("event_actor").asc_nulls_last(),
        F.col("event_event_at_decile").asc_nulls_last(),
    )
)
