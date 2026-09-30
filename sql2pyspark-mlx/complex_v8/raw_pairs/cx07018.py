from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_ties = Window.orderBy(F.col("event_at").desc_nulls_first())
w_ticket_id_by_event_at_full = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_event_type_by_even = Window.partitionBy("ticket_id", "event_type").orderBy(F.col("event_at").desc())

filtered = (
    df_ticket_events
    .filter((F.month(F.col("event_at")).isin(1, 2)) | (F.col("event_type").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
        F.lower(F.trim(F.col("event_type"))).alias("event_type_clean"),
    )
)

windowed = (
    filtered
    .select(
        "*",
        F.dense_rank().over(w_all_by_event_at_ties).alias("event_at_dense_rank"),
        F.first("event_at").over(w_ticket_id_by_event_at_full).alias("first_event_at"),
        F.rank().over(w_ticket_id_event_type_by_even).alias("event_at_rank"),
    )
)

with_details_ticket = (
    windowed.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.actor",
        "s.actor_slug",
        "s.event_at_dense_rank",
        "s.first_event_at",
        "s.event_at_rank",
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.agent_id").alias("ticket_agent_id"),
    )
)

result = (
    with_details_ticket
    .select("id", "event_at_rank", "event_at_dense_rank")
    .unpivot(
        ids=["id"],
        values=["event_at_rank", "event_at_dense_rank"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
