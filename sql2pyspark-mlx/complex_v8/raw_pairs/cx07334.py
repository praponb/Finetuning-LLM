from pyspark.sql import functions as F

base_events = (
    df_ticket_events
    .filter((F.col("ticket_id") <= 7) & (F.col("event_type").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.initcap(F.trim(F.col("event_type"))).alias("event_type_clean"),
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
        F.concat(F.col("actor"), F.lit("#"), F.col("ticket_id").cast("string")).alias("actor_ref"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.min("event_at").alias("first_event_at_2"),
        F.count(F.when(F.col("actor") == "system", 1)).alias("event_actor_count"),
    )
)

with_stats = (
    base_events.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.first_event_at",
        "t.first_event_at_2",
        F.coalesce(F.col("t.event_actor_count"), F.lit(0)).alias("event_actor_count"),
    )
)

grouped = (
    with_stats
    .groupBy(
        F.date_trunc("month", "first_event_at_2").alias("first_event_at_2_month"),
        "event_type",
        F.year("event_at").alias("event_at_year"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("actor").alias("distinct_actors"),
        F.min("first_event_at_2").alias("first_first_event_at_2"),
        F.round(F.sum("event_actor_count") / F.nullif(F.sum("actor_len"), F.lit(0)), 3).alias("event_actor_count_to_actor_len_ratio"),
    )
)

result = (
    grouped
    .select(
        "first_event_at_2_month",
        "event_type",
        "cnt",
        "event_actor_count_to_actor_len_ratio",
        F.substring(F.trim(F.col("event_type")), 1, 4).alias("event_type_prefix"),
    )
    .orderBy(
        F.col("event_actor_count_to_actor_len_ratio").desc_nulls_first(),
        F.col("cnt").asc_nulls_last(),
    )
)
