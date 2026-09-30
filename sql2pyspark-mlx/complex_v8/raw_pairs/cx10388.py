from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_id_ticket_id_by_event = Window.partitionBy("event_id", "ticket_id").orderBy(F.col("event_at").desc(), "id", "event_id")
w_event_id_actor_or_unknown_by = Window.partitionBy("event_id", "actor_or_unknown").orderBy("event_event_at", "id", "event_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_event_type_event_id_by = Window.partitionBy("event_event_type", "event_id").orderBy(F.col("event_event_at").desc(), "id", "event_id")
w_event_id_actor_or_unknown_by_2 = Window.partitionBy("event_id", "actor_or_unknown").orderBy("event_event_at", "id", "event_id")

recent_events = (
    df_ticket_events
    .filter((F.col("actor").isin("system", "agent")) | (F.col("ticket_id") <= 14))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("event_type").isin("reopened", "created"), "group_a").when(F.col("event_type") == "replied", "group_b").otherwise("other").alias("event_type_group"),
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
    )
)

detail_rows = (
    recent_events.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type_group",
        "s.actor_or_unknown",
        F.col("t.id").alias("event_id"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.event_at").alias("event_event_at"),
    )
)

windowed = (
    detail_rows
    .select(
        "id",
        "ticket_id",
        "actor_or_unknown",
        "event_id",
        "event_event_type",
        F.lead("event_at", 3).over(w_event_id_ticket_id_by_event).alias("next3_event_at"),
        F.last("event_event_at", ignorenulls=True).over(w_event_id_actor_or_unknown_by).alias("last_known_event_event_at"),
        F.lag("event_event_at").over(w_event_event_type_event_id_by).alias("prev_event_event_at"),
        F.ntile(4).over(w_event_id_actor_or_unknown_by_2).alias("event_event_at_quartile"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.max("event_at").alias("last_event_at"))
)

profiled = (
    windowed.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.last_event_at")
)

result = (
    profiled
    .select(
        "id",
        "ticket_id",
        "event_id",
        "event_event_type",
        "next3_event_at",
        "last_known_event_event_at",
        "event_event_at_quartile",
        F.lower(F.trim(F.col("event_event_type"))).alias("event_event_type_clean"),
    )
    .orderBy(F.col("event_event_type_clean").asc())
)
