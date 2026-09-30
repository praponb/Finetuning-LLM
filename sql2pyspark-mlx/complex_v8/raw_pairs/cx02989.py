from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_by_event_at_runnin = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_actor_by_event_at = Window.partitionBy("ticket_id", "actor").orderBy(F.col("event_at").desc())
w_ticket_id_actor_by_event_at_2 = Window.partitionBy("ticket_id", "actor").orderBy(F.col("event_at").desc(), "id")
w_ticket_id_by_event_at_ties = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc())

filtered = df_ticket_events.filter(F.to_date(F.col("event_at")) < F.lit("2025-02-18").cast("date"))

ranked = (
    filtered
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.first("event_at", ignorenulls=True).over(w_ticket_id_by_event_at_runnin).alias("first_known_event_at"),
        F.dense_rank().over(w_ticket_id_actor_by_event_at).alias("event_at_dense_rank"),
        F.rank().over(w_ticket_id_actor_by_event_at).alias("event_at_rank"),
        F.round(F.cume_dist().over(w_ticket_id_actor_by_event_at_2), 3).alias("event_at_cume_dist"),
        F.ntile(10).over(w_ticket_id_actor_by_event_at_2).alias("event_at_decile"),
        F.round(F.percent_rank().over(w_ticket_id_by_event_at_ties), 3).alias("event_at_percent_rank"),
    )
)

filtered_2 = (
    ranked.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_at") < F.lit("2025-02-18 11:00:00").cast("timestamp")), "left_semi")
)

joined = (
    filtered_2.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.size(F.col("t.tags")) >= 2)
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.first_known_event_at",
        "s.event_at_dense_rank",
        "s.event_at_rank",
        "s.event_at_cume_dist",
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.tags").alias("ticket_tags"),
    )
)

result = (
    joined
    .filter(~F.array_contains(F.col("ticket_tags"), "outage"))
    .select(
        "id",
        "ticket_id",
        "event_type",
        "ticket_tags",
        F.round(F.col("event_at_dense_rank") / F.nullif(F.col("event_at_cume_dist"), F.lit(0)), 3).alias("event_at_dense_rank_per_event_at_cume_dist"),
    )
    .orderBy(F.col("ticket_id").asc_nulls_last(), "id")
    .limit(5)
)
