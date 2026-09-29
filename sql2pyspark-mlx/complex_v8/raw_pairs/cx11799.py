from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_event_at_to_date_by_ev = Window.partitionBy("actor", F.to_date("event_at")).orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_event_at_to_date_by_ev_2 = Window.partitionBy("actor", F.to_date("event_at")).orderBy(F.col("event_at").desc(), "id")
w_event_type_ticket_id_by_even = Window.partitionBy("event_type", "ticket_id").orderBy(F.col("event_at").asc_nulls_last())
w_event_type_ticket_id_by_even_2 = Window.partitionBy("event_type", "ticket_id").orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_ticket_id_by_even_3 = Window.partitionBy("event_type", "ticket_id").orderBy(F.col("event_at").asc_nulls_last(), "id")

base_events = (
    df_ticket_events
    .filter((F.col("actor").isin("agent", "system")) | (F.col("ticket_id") <= 45))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

windowed = (
    base_events
    .select(
        "*",
        F.nth_value("event_at", 2).over(w_actor_event_at_to_date_by_ev).alias("second_event_at"),
        F.dense_rank().over(w_actor_event_at_to_date_by_ev_2).alias("event_at_dense_rank"),
        F.round(F.percent_rank().over(w_event_type_ticket_id_by_even), 3).alias("event_at_percent_rank"),
        F.lead("event_at").over(w_actor_event_at_to_date_by_ev_2).alias("next_event_at"),
        F.nth_value("event_at", 2).over(w_event_type_ticket_id_by_even_2).alias("second_event_at_2"),
        F.round(F.percent_rank().over(w_event_type_ticket_id_by_even_3), 3).alias("event_at_percent_rank_2"),
    )
)

joined = (
    windowed.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .filter(F.col("t.priority") == "P3")
    .select(
        "s.*",
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.priority").alias("ticket_priority"),
        "su.agent_name",
    )
)

enriched = (
    joined
    .filter(F.col("event_at_dense_rank") <= 9)
    .withColumn("event_at_dense_rank_distance", F.abs(F.col("event_at_dense_rank") - 7))
)

qualified = (
    enriched
    .crossJoin(enriched.agg(F.avg("event_at_dense_rank_distance").alias("overall_avg_event_at_dense_rank_distance")))
    .filter(F.col("event_at_dense_rank_distance") > F.col("overall_avg_event_at_dense_rank_distance"))
    .drop("overall_avg_event_at_dense_rank_distance")
)

with_activity = (
    qualified.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.array_contains(F.col("t.tags"), "refund")), "left_anti")
)

result = (
    with_activity
    .filter(F.col("next_event_at") >= F.lit("2025-01-25 10:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "event_at_percent_rank",
        "next_event_at",
        "second_event_at_2",
        "event_at_percent_rank_2",
        "ticket_tags",
        "agent_name",
        "event_at_dense_rank_distance",
    )
    .orderBy(F.col("next_event_at").desc())
)
