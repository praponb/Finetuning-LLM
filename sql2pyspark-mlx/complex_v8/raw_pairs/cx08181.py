from pyspark.sql import functions as F

scoped_events = (
    df_ticket_events
    .filter((F.month(F.col("event_at")).isin(2, 3)) & (F.col("ticket_id") >= 50))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
        F.lower(F.trim(F.col("event_type"))).alias("event_type_clean"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.count(F.when(F.col("event_type").isNotNull(), 1)).alias("event_event_type_count"),
        F.max("event_at").alias("last_event_at_2"),
    )
)

profiled = (
    scoped_events.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.last_event_at",
        F.coalesce(F.col("t.event_event_type_count"), F.lit(0)).alias("event_event_type_count"),
        "t.last_event_at_2",
    )
)

with_activity = (
    profiled.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left_semi")
    .filter(F.col("last_event_at") < F.lit("2025-02-13 09:00:00").cast("timestamp"))
)

tagged = (
    with_activity
    .filter(F.month(F.col("last_event_at")).isin(2, 3))
    .withColumn("last_event_at_2_dayofweek", F.dayofweek(F.col("last_event_at_2")))
)

result = (
    tagged
    .select(
        "id",
        F.col("last_event_at_2_dayofweek").alias("m_last_event_at_2_dayofweek"),
        F.col("event_event_type_count").alias("m_event_event_type_count"),
    )
    .unpivot(
        ids=["id"],
        values=["m_last_event_at_2_dayofweek", "m_event_event_type_count"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "metric")
)
