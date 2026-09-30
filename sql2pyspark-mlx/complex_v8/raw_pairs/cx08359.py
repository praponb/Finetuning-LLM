from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_actor_count_ticket_id = Window.partitionBy("event_actor_count", "ticket_id").orderBy(F.col("event_actor_count").desc())
w_last_event_at_2_month_event = Window.partitionBy(F.date_trunc("month", "last_event_at_2"), "event_type")

recent_events = (
    df_ticket_events
    .filter((F.col("event_at") < F.lit("2025-03-08 09:00:00").cast("timestamp")) & (~F.col("event_type").isin("replied", "resolved")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.substring(F.trim(F.col("event_type")), 1, 3).alias("event_type_prefix"),
        F.date_trunc("quarter", F.col("event_at")).alias("event_at_quarter"),
        F.month(F.col("event_at")).alias("event_at_month"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.max("event_at").alias("last_event_at_2"),
        F.count(F.when(F.col("actor").isin("system", "agent"), 1)).alias("event_actor_count"),
    )
)

with_stats = (
    recent_events.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.last_event_at",
        "t.last_event_at_2",
        F.coalesce(F.col("t.event_actor_count"), F.lit(0)).alias("event_actor_count"),
    )
)

sequenced = (
    with_stats
    .withColumn("event_actor_count_rank", F.rank().over(w_event_actor_count_ticket_id))
    .withColumn("distinct_ticket_id_in_last_event_at_2_month_even", F.size(F.collect_set("ticket_id").over(w_last_event_at_2_month_event)))
)

result = (
    sequenced
    .cube("actor", "ticket_id", "event_at_month")
    .agg(
        F.sum("event_actor_count").alias("total_event_actor_count"),
        F.max("event_at_quarter").alias("last_event_at_quarter"),
    )
    .select(
        F.coalesce(F.col("actor"), F.lit("ALL")).alias("actor"),
        "ticket_id",
        "event_at_month",
        "total_event_actor_count",
        "last_event_at_quarter",
    )
    .orderBy(
        F.col("actor").asc_nulls_last(),
        F.col("ticket_id").asc_nulls_last(),
        F.col("event_at_month").asc_nulls_last(),
    )
)
