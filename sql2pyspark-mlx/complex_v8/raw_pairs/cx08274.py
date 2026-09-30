from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_actor_by_event_at = Window.partitionBy("event_type", "actor").orderBy("event_at", "id")
w_event_type_actor_by_event_at_2 = Window.partitionBy("event_type", "actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base_events = (
    df_ticket_events
    .filter(~F.col("event_type").isin("resolved", "escalated"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.trim(F.col("event_type"))).alias("event_type_clean"),
    )
)

windowed = (
    base_events
    .select(
        "*",
        F.rank().over(w_event_type_actor_by_event_at).alias("event_at_rank"),
        F.ntile(3).over(w_event_type_actor_by_event_at).alias("event_at_tercile"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_event_type_actor_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.first("event_at").over(w_event_type_actor_by_event_at_2).alias("first_event_at"),
    )
)

result = (
    windowed
    .groupingSets(
        [["actor", "event_type"], ["actor"], ["event_type"], []],
        "actor", "event_type",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("minutes_since_prev_event_at") / F.nullif(F.sum("event_at_rank"), F.lit(0)), 3).alias("minutes_since_prev_event_at_to_event_at_rank_rat"),
    )
    .select(
        F.coalesce(F.col("actor"), F.lit("ALL")).alias("actor"),
        F.coalesce(F.col("event_type"), F.lit("ALL")).alias("event_type"),
        "distinct_ids",
        "minutes_since_prev_event_at_to_event_at_rank_rat",
    )
    .orderBy(F.col("actor").asc_nulls_last(), F.col("event_type").asc_nulls_last())
)
