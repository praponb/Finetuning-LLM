from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_count_running = Window.orderBy("event_count", "id", "event_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_id_event_actor_by_even = Window.partitionBy("event_id", "event_actor").orderBy("event_count", "id", "event_id")
w_event_id_event_event_at_to_d = Window.partitionBy("event_id", F.to_date("event_event_at")).orderBy("event_event_at", "id", "event_id")
w_all_by_event_count = Window.orderBy("event_count", "id", "event_id")

recent = (
    df_ticket_events
    .filter(F.col("event_type") == "reopened")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("event_type").isin("assigned", "replied"), "group_a").when(F.col("event_type") == "created", "group_b").otherwise("other").alias("event_type_group"),
        F.lower(F.trim(F.col("actor"))).alias("actor_clean"),
        F.concat(F.col("actor"), F.lit("#"), F.col("id").cast("string")).alias("actor_ref"),
    )
)

detail_rows = (
    recent.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor_ref",
        F.col("t.id").alias("event_id"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_at").alias("event_event_at"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.count("*").alias("event_count"))
)

with_stats = (
    detail_rows.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

labeled = (
    with_stats
    .filter(F.col("event_id").isNotNull())
    .select(
        "*",
        F.coalesce(F.col("event_event_type"), F.lit("UNKNOWN")).alias("event_event_type_or_unknown"),
    )
)

ranked = (
    labeled
    .select(
        "*",
        F.first("event_at", ignorenulls=True).over(w_all_by_event_count_running).alias("first_known_event_at"),
        F.round(F.percent_rank().over(w_event_id_event_actor_by_even), 3).alias("event_count_percent_rank"),
        F.row_number().over(w_event_id_event_event_at_to_d).alias("seq_no"),
        F.ntile(5).over(w_all_by_event_count).alias("event_count_quintile"),
    )
)

result = (
    ranked
    .filter(F.col("actor_ref").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor_ref",
        "event_id",
        "event_actor",
        "event_event_at",
        "event_count",
        "first_known_event_at",
        "seq_no",
        "event_count_quintile",
    )
    .orderBy("actor_ref", F.col("event_event_at").desc())
)
