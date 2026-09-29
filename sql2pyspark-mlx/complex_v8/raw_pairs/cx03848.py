from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_ties = Window.orderBy("event_at")
w_event_type_actor_by_event_at = Window.partitionBy("event_type", "actor").orderBy(F.col("event_at").desc(), "id")
w_event_type_ticket_id_by_even = Window.partitionBy("event_type", "ticket_id").orderBy("event_at_cume_dist", "id").rowsBetween(-3, Window.currentRow)
w_event_type_ticket_id_by_even_2 = Window.partitionBy("event_type", "ticket_id").orderBy("event_at_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_ticket_events
    .filter((F.col("event_type").isin("escalated", "replied", "reopened")) & (F.col("event_at") >= F.lit("2025-01-25 13:00:00").cast("timestamp")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

labeled = (
    base
    .withColumn("event_type_slug", F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")))
)

with_windows = (
    labeled
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_event_at_ties), 3).alias("event_at_cume_dist"),
        F.lag("event_at", 3).over(w_event_type_actor_by_event_at).alias("prev3_event_at"),
    )
)

ranked = (
    with_windows
    .select(
        "id",
        "event_at",
        "actor",
        "event_type_slug",
        "prev3_event_at",
        F.round(F.avg("event_at_cume_dist").over(w_event_type_ticket_id_by_even), 2).alias("event_at_cume_dist_avg_4"),
        F.count(F.lit(1)).over(w_event_type_ticket_id_by_even_2).alias("running_count"),
    )
)

result = (
    ranked
    .select(
        "id",
        "event_at",
        "event_type_slug",
        "prev3_event_at",
        "event_at_cume_dist_avg_4",
        "running_count",
    )
    .orderBy(F.col("running_count").desc())
)
