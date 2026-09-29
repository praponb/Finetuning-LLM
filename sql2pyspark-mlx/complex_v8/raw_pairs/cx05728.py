from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("ticket_id", "event_type").orderBy("event_at", "id")

cleaned_events = (
    df_ticket_events
    .filter((F.col("actor").isNotNull()) | (F.month(F.col("event_at")).isin(1, 3)))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("event_type").isin("replied", "created"), "group_a").when(F.col("event_type") == "resolved", "group_b").otherwise("other").alias("event_type_group"),
        F.concat(F.col("actor"), F.lit("#"), F.col("id").cast("string")).alias("actor_ref"),
        F.substring(F.trim(F.col("event_type")), 1, 3).alias("event_type_prefix"),
    )
)

latest = (
    cleaned_events
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

with_details = (
    latest.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.size(F.col("t.tags")) >= 1), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.event_type_group",
        "s.actor_ref",
        "s.event_type_prefix",
        F.col("t.tags").alias("ticket_tags"),
    )
)

exploded_detail = (
    with_details.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type_group",
        "s.actor_ref",
        "s.ticket_tags",
        F.col("t.id").alias("event_id"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.actor").alias("event_actor"),
        "t.event_at",
    )
)

result = (
    exploded_detail
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_type_group",
        "ticket_tags",
        "event_id",
        "event_event_type",
        "event_at",
        F.year(F.col("event_at")).alias("event_at_year"),
    )
    .orderBy(F.col("id").desc(), F.col("event_event_type").asc_nulls_last())
)
