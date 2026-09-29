from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("actor", "event_type").orderBy("event_at", "id")

valid_events = (
    df_ticket_events
    .filter((F.col("ticket_id") >= 59) & (~F.col("actor").isin("system", "agent")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.trim(F.col("actor"))).alias("actor_clean"),
        F.date_format(F.col("event_at"), "yyyy-MM-dd").alias("event_at_label"),
    )
)

latest = (
    valid_events
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

exploded_detail = (
    latest.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        F.col("t.id").alias("event_id"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_type").alias("event_event_type"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.count("*").alias("event_count"),
        F.max("event_at").alias("last_event_at_2"),
    )
)

summarized_rows = (
    exploded_detail.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.last_event_at",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        "t.last_event_at_2",
    )
)

event_stats_2 = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at_3"),
        F.count(F.when(F.col("actor").isin("agent", "customer"), 1)).alias("event_actor_count"),
    )
)

with_counts = (
    summarized_rows.alias("s")
    .join(event_stats_2.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.last_event_at_3",
        F.coalesce(F.col("t.event_actor_count"), F.lit(0)).alias("event_actor_count"),
    )
)

result = (
    with_counts
    .groupingSets(
        [["event_id", "ticket_id", "event_count"], ["event_id"], ["ticket_id"]],
        "event_id", "ticket_id", "event_count",
    )
    .agg(
        F.grouping("event_id").alias("is_total_event_id"),
        F.count("*").alias("n_rows"),
    )
    .orderBy(
        F.col("event_id").asc_nulls_last(),
        F.col("ticket_id").asc_nulls_last(),
        F.col("event_count").asc_nulls_last(),
    )
)
