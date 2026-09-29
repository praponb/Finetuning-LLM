from pyspark.sql import functions as F

prepared_events = (
    df_ticket_events
    .filter((~F.col("actor").isin("agent", "system")) | (F.hour(F.col("event_at")).between(9, 13)))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("event_type").isin("replied", "reopened"), "group_a").when(F.col("event_type") == "resolved", "group_b").otherwise("other").alias("event_type_group"),
        F.initcap(F.trim(F.col("actor"))).alias("actor_clean"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.max("event_at").alias("last_event_at"),
    )
)

profiled = (
    prepared_events.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.first_event_at", "t.last_event_at")
)

with_activity = (
    profiled.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.actor") != "customer"), "left_semi")
    .filter(F.col("event_type_group") == "group_a")
)

result = (
    with_activity
    .groupingSets(
        [["ticket_id", "event_type_group"], ["ticket_id"], ["event_type_group"], []],
        "ticket_id", "event_type_group",
    )
    .agg(
        F.array_sort(F.collect_set("event_type")).alias("event_type_list"),
        F.count("*").alias("cnt"),
    )
    .orderBy(
        F.col("ticket_id").asc_nulls_last(),
        F.col("event_type_group").asc_nulls_last(),
    )
)
