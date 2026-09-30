from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("agent_id").orderBy(F.col("event_count").desc(), F.col("id").desc())

prepared = (
    df_tickets
    .filter((F.col("channel").isin("web", "chat", "email")) & (F.col("closed_at") >= F.lit("2025-02-04 23:00:00").cast("timestamp")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "csat",
        "tags",
        (F.col("requester_id") + F.col("csat")).alias("requester_id_plus_csat"),
        (F.col("csat") + F.col("requester_id")).alias("csat_plus_requester_id"),
        F.date_format(F.col("closed_at"), "MMM yyyy").alias("closed_at_label"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count("*").alias("event_count"),
        F.count(F.when(~F.col("actor").isin("agent", "system"), 1)).alias("event_actor_count"),
    )
)

profiled = (
    prepared.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        F.coalesce(F.col("t.event_actor_count"), F.lit(0)).alias("event_actor_count"),
    )
)

latest = (
    profiled
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .select(
        "id",
        F.col("requester_id").alias("m_requester_id"),
        F.col("requester_id_plus_csat").alias("requester_id_plus_csat_v"),
        F.col("event_actor_count").alias("event_actor_count_v"),
    )
    .unpivot(
        ids=["id"],
        values=["m_requester_id", "requester_id_plus_csat_v", "event_actor_count_v"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .orderBy("id", "metric")
)
