from pyspark.sql import functions as F

cleaned = (
    df_ticket_events
    .filter(F.col("event_type") != "resolved")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_trunc("month", F.col("event_at")).alias("event_at_month"),
        F.date_trunc("quarter", F.col("event_at")).alias("event_at_quarter"),
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
    )
)

totals = (
    cleaned
    .filter(F.col("event_at_quarter") < F.lit("2025-02-13 09:00:00").cast("timestamp"))
    .groupBy("event_type")
    .agg(
        F.sum(F.when(F.col("actor").isNotNull(), 1).otherwise(0)).alias("n_actor_is_not_null"),
        F.max("event_at_month").alias("last_event_at_month"),
        F.count("*").alias("cnt"),
    )
)

tagged = (
    totals
    .select(
        "event_type",
        "n_actor_is_not_null",
        "last_event_at_month",
        F.dayofweek(F.col("last_event_at_month")).alias("last_event_at_month_dayofweek"),
        F.abs(F.col("cnt") - 6).alias("cnt_distance"),
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
    )
)

result = (
    tagged
    .select(
        "event_type",
        F.col("last_event_at_month_dayofweek").alias("m_last_event_at_month_dayofweek"),
        F.col("n_actor_is_not_null").alias("m_n_actor_is_not_null"),
        F.col("cnt_distance").alias("cnt_distance_v"),
    )
    .unpivot(
        ids=["event_type"],
        values=["m_last_event_at_month_dayofweek", "m_n_actor_is_not_null", "cnt_distance_v"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .orderBy("event_type", "field")
)
