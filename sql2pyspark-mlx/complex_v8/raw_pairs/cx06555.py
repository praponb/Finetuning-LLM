from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("ticket_id", "event_event_type").orderBy(F.col("event_event_at").desc(), F.col("id").desc(), F.col("event_id").desc())

recent = (
    df_ticket_events
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("event_type").isin("assigned", "reopened"), "group_a").when(F.col("event_type") == "created", "group_b").otherwise("other").alias("event_type_group"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.substring(F.trim(F.col("actor")), 1, 4).alias("actor_prefix"),
    )
)

line_level = (
    recent.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.event_type_group",
        "s.actor_prefix",
        F.col("t.id").alias("event_id"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.actor").alias("event_actor"),
    )
)

latest = (
    line_level
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest
    .groupingSets(
        [["event_event_type", "event_type"], ["event_event_type"]],
        "event_event_type", "event_type",
    )
    .agg(
        F.grouping("event_event_type").alias("is_total_event_event_type"),
        F.count("*").alias("cnt"),
        F.countDistinct("event_event_type").alias("distinct_event_event_types"),
    )
)
