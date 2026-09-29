from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at = Window.orderBy(F.col("event_at").desc(), "id", "event_id")

valid = (
    df_ticket_events
    .filter(F.col("actor").isin("agent", "customer"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
    )
)

with_children_events = (
    valid.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        F.col("t.id").alias("event_id"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_type").alias("event_event_type"),
    )
)

with_trends = (
    with_children_events
    .select(
        "*",
        F.row_number().over(w_all_by_event_at).alias("event_at_seq"),
        F.ntile(4).over(w_all_by_event_at).alias("event_at_quartile"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.min("event_at").alias("first_event_at"),
        F.max("event_at").alias("last_event_at_2"),
    )
)

profiled = (
    with_trends.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.last_event_at",
        "t.first_event_at",
        "t.last_event_at_2",
    )
)

result = (
    profiled
    .select(
        "id",
        "event_id",
        "event_at_seq",
        "event_at_quartile",
    )
    .unpivot(
        ids=["id", "event_id"],
        values=["event_at_seq", "event_at_quartile"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "event_id", "field")
)
