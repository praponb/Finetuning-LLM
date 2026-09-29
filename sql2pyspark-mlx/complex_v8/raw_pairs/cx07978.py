from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_event_at_runnin = Window.orderBy(F.col("event_event_at").desc_nulls_first(), "id", "event_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type_by_event_event_at = Window.partitionBy("event_type").orderBy("event_event_at")

valid_events = (
    df_ticket_events
    .filter(F.col("ticket_id") >= 24)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("actor").isin("system", "agent"), "group_a").when(F.col("actor") == "customer", "group_b").otherwise("other").alias("actor_group"),
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
    )
)

line_level_events = (
    valid_events.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        F.col("t.id").alias("event_id"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.event_type").alias("event_event_type"),
    )
)

sequenced = (
    line_level_events
    .select(
        "id",
        "actor",
        "event_id",
        "event_event_at",
        F.first("event_event_at", ignorenulls=True).over(w_all_by_event_event_at_runnin).alias("first_known_event_event_at"),
        F.dense_rank().over(w_event_type_by_event_event_at).alias("event_event_at_dense_rank"),
    )
)

combined_details = (
    sequenced.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.id") == F.col("s.event_id"), "inner")
    .filter(F.col("t.event_at") >= F.lit("2025-01-31 13:00:00").cast("timestamp"))
    .select("s.*", "t.event_at")
)

joined = (
    combined_details.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.id") == F.col("s.event_id"), "left")
    .join(df_tickets.alias("ti"), F.col("ti.id") == F.col("t.ticket_id"), "left")
    .select(
        "s.id",
        "s.event_id",
        "s.event_event_at",
        "s.first_known_event_event_at",
        "s.event_event_at_dense_rank",
        "s.event_at",
        "t.event_type",
        F.col("t.ticket_id").alias("event_ticket_id"),
        F.col("t.actor").alias("event_actor"),
        F.col("ti.tags").alias("ticket_tags"),
        F.col("ti.channel").alias("ticket_channel"),
    )
)

result = (
    joined
    .select("ticket_tags")
    .distinct()
)
