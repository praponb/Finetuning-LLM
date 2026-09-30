from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_event_type_by_even = Window.partitionBy("ticket_id", "event_type").orderBy(F.col("event_count").desc())
w_ticket_id_event_type_by_even_2 = Window.partitionBy("ticket_id", "event_type").orderBy(F.col("event_count").desc(), "id")

valid_events = (
    df_ticket_events
    .filter(F.col("actor").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
    )
)

without_matches = (
    valid_events.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.hour(F.col("t.closed_at")).between(8, 14)) & (F.col("t.priority").isNotNull()), "left_anti")
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count("*").alias("event_count"),
        F.max("event_at").alias("last_event_at"),
        F.count("*").alias("event_count_2"),
    )
)

with_stats = (
    without_matches.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        "t.last_event_at",
        F.coalesce(F.col("t.event_count_2"), F.lit(0)).alias("event_count_2"),
    )
)

with_trends = (
    with_stats
    .withColumn("event_count_cume_dist", F.round(F.cume_dist().over(w_ticket_id_event_type_by_even), 3))
    .withColumn("event_count_rank", F.rank().over(w_ticket_id_event_type_by_even_2))
)

result = (
    with_trends
    .select("id", "event_count", "event_count_rank")
    .unpivot(
        ids=["id"],
        values=["event_count", "event_count_rank"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
)
