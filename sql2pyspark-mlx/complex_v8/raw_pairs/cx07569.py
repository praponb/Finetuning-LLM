from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_actor_by_event_at = Window.partitionBy("ticket_id", "actor").orderBy(F.col("event_at").desc())
w_ticket_id_actor_by_event_at_2 = Window.partitionBy("ticket_id", "actor").orderBy(F.col("event_at").desc(), "id")

cleaned = (
    df_ticket_events
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat(F.col("actor"), F.lit("#"), F.col("ticket_id").cast("string")).alias("actor_ref"),
    )
)

sequenced = (
    cleaned
    .withColumn("event_at_percent_rank", F.round(F.percent_rank().over(w_ticket_id_actor_by_event_at), 3))
    .withColumn("prev2_event_at", F.lag("event_at", 2).over(w_ticket_id_actor_by_event_at_2))
)

detail_rows_events = (
    sequenced.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_at_percent_rank",
        "s.prev2_event_at",
        F.col("t.id").alias("event_id"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.event_type").alias("event_event_type"),
    )
)

combined_details = (
    detail_rows_events.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.id") == F.col("s.event_id"), "left")
    .join(df_tickets.alias("ti"), F.col("ti.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.col("t.ticket_id").alias("event_ticket_id"),
        F.col("t.event_type").alias("event_event_type_2"),
        F.col("t.actor").alias("event_actor"),
        F.col("ti.csat").alias("ticket_csat"),
    )
)

result = (
    combined_details
    .groupingSets(
        [["actor", "event_type"], ["actor"], ["event_type"], []],
        "actor", "event_type",
    )
    .agg(F.sum("event_at_percent_rank").alias("total_event_at_percent_rank"))
    .select(
        F.coalesce(F.col("actor"), F.lit("ALL")).alias("actor"),
        F.coalesce(F.col("event_type"), F.lit("ALL")).alias("event_type"),
        "total_event_at_percent_rank",
    )
    .orderBy(F.col("actor").asc_nulls_last(), F.col("event_type").asc_nulls_last())
)
