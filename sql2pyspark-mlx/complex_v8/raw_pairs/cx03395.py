from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("agent_id", "priority").orderBy("requester_id", "id")

prepared = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.round((F.unix_timestamp(F.col("opened_at")) - F.unix_timestamp(F.col("closed_at"))) / 3600, 1).alias("hours_closed_at_to_opened_at"),
        F.when(F.col("requester_id") >= 17, "large").when(F.col("requester_id") >= 5, "medium").otherwise("small").alias("requester_id_tier"),
    )
)

latest = (
    prepared
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

flattened = (
    latest
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "hours_closed_at_to_opened_at",
        "requester_id_tier",
        F.explode("tags").alias("tag"),
    )
)

screened = (
    flattened.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.event_type") == "assigned"), "left_anti")
    .filter(F.col("agent_id").isNotNull())
)

detail_rows = (
    screened.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.closed_at",
        "s.priority",
        "s.requester_id_tier",
        "s.tag",
        F.col("t.id").alias("event_id"),
        "t.event_type",
        F.col("t.actor").alias("event_actor"),
        "t.event_at",
    )
)

result = (
    detail_rows
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "requester_id_tier",
        "tag",
        "event_id",
        "event_type",
        "event_actor",
        F.date_format(F.col("event_at"), "yyyy-MM-dd").alias("event_at_label"),
    )
    .orderBy(F.col("tag").desc(), "id", "event_id")
    .limit(20)
)
