from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("event_type", "actor").orderBy(F.col("event_at").desc(), F.col("id").desc())

recent = df_ticket_events.filter(F.col("ticket_id").isNotNull())

deduped = (
    recent
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

combined_details = (
    deduped.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (~F.array_contains(F.col("t.tags"), "upgrade")), "inner")
    .filter(F.col("t.requester_id") > 2)
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.requester_id").alias("ticket_requester_id"),
    )
)

result = (
    combined_details
    .groupingSets(
        [["ticket_id", "actor", "event_type"], ["ticket_id"], []],
        "ticket_id", "actor", "event_type",
    )
    .agg(
        F.count(F.when(F.col("event_type") == "created", 1)).alias("n_event_type_created"),
    )
    .orderBy(
        F.col("ticket_id").asc_nulls_last(),
        F.col("actor").asc_nulls_last(),
        F.col("event_type").asc_nulls_last(),
    )
)
