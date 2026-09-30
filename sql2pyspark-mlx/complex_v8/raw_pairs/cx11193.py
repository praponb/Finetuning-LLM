from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_id_by_event_at_ties = Window.partitionBy("event_id").orderBy(F.col("event_at").desc())

valid = (
    df_ticket_events
    .filter((~F.col("actor").isin("system", "agent")) & (F.to_date(F.col("event_at")) < F.lit("2025-03-02").cast("date")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.date_format(F.col("event_at"), "yyyy-MM").alias("event_at_label"),
        F.when(F.col("event_type").isin("reopened", "replied"), "group_a").when(F.col("event_type") == "escalated", "group_b").otherwise("other").alias("event_type_group"),
    )
)

detail_rows_events = (
    valid.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_at_label",
        "s.event_type_group",
        F.col("t.id").alias("event_id"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_at").alias("event_event_at"),
    )
)

windowed = (
    detail_rows_events
    .select(
        "id",
        "ticket_id",
        "event_type",
        "event_type_group",
        "event_id",
        "event_actor",
        "event_event_at",
        F.rank().over(w_event_id_by_event_at_ties).alias("event_at_rank"),
    )
)

result = (
    windowed
    .groupingSets(
        [["ticket_id", "event_id"], ["ticket_id"]],
        "ticket_id", "event_id",
    )
    .agg(F.countDistinct("ticket_id").alias("distinct_ticket_ids"))
    .orderBy(F.col("ticket_id").asc_nulls_last(), F.col("event_id").asc_nulls_last())
)
