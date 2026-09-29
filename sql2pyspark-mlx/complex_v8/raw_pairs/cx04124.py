from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("actor", "event_type_group").orderBy(F.col("event_at").desc(), F.col("id").desc())

scoped_events = (
    df_ticket_events
    .filter((F.col("event_type").isNotNull()) & (F.col("actor") == "agent"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

annotated = (
    scoped_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.upper(F.trim(F.col("event_type"))).alias("event_type_clean"),
        F.when(F.col("event_type").isin("escalated", "reopened"), "group_a").when(F.col("event_type") == "created", "group_b").otherwise("other").alias("event_type_group"),
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
    )
)

latest_per_key = (
    annotated
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.count("*").alias("event_count"))
)

summarized_rows = (
    latest_per_key.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

result = (
    summarized_rows
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        "event_type_clean",
        "event_type_slug",
        "event_count",
        F.abs(F.col("event_count") - 3).alias("event_count_distance"),
    )
    .orderBy(F.col("actor").desc())
)
