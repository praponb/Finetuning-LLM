from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_event_type_by_event_at = Window.partitionBy("actor", "event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_event_type_by_event_at_2 = Window.partitionBy("actor", "event_type").orderBy("event_at", "id")

scoped = (
    df_ticket_events
    .filter(F.col("event_at") >= F.lit("2025-01-25 10:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.trim(F.col("actor"))).alias("actor_clean"),
    )
)

with_trends = (
    scoped
    .select(
        "id",
        "event_at",
        "event_type",
        F.last("event_at").over(w_actor_event_type_by_event_at).alias("last_event_at"),
        F.first("event_at").over(w_actor_event_type_by_event_at).alias("first_event_at"),
        F.ntile(4).over(w_actor_event_type_by_event_at_2).alias("event_at_quartile"),
        F.row_number().over(w_actor_event_type_by_event_at_2).alias("seq_no"),
    )
)

result = (
    with_trends
    .rollup("event_at_quartile", "event_type")
    .agg(
        F.round(F.sum("event_at_quartile") / F.nullif(F.sum("seq_no"), F.lit(0)), 3).alias("event_at_quartile_to_seq_no_ratio"),
    )
    .select(
        "event_at_quartile",
        F.coalesce(F.col("event_type"), F.lit("ALL")).alias("event_type"),
        "event_at_quartile_to_seq_no_ratio",
    )
    .orderBy(
        F.col("event_at_quartile").asc_nulls_last(),
        F.col("event_type").asc_nulls_last(),
    )
)
