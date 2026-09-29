from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_by_event_at = Window.partitionBy("event_type").orderBy(F.col("event_at").desc(), "id", "event_id")
w_actor_event_id_by_event_at_r = Window.partitionBy("actor", "event_id").orderBy(F.col("event_at").desc(), "id", "event_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type_by_event_at_ties = Window.partitionBy("event_type").orderBy(F.col("event_at").desc())
w_event_at_dense_rank_event_at = Window.partitionBy("event_at_dense_rank", "event_at_rank")
w_event_actor_actor_by_last_kn = Window.partitionBy("event_actor", "actor").orderBy(F.col("last_known_event_event_at").desc(), "id", "event_id")

scoped_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

detail_rows_events = (
    scoped_events.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.hour(F.col("t.event_at")).between(8, 13)), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.id").alias("event_id"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_at").alias("event_event_at"),
    )
)

filtered = (
    detail_rows_events.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.closed_at") >= F.lit("2025-02-08 15:00:00").cast("timestamp")) & (F.col("t.csat") < 4), "left_semi")
)

with_trends = (
    filtered
    .select(
        "*",
        F.dense_rank().over(w_event_type_by_event_at).alias("event_at_dense_rank"),
        F.last("event_event_at", ignorenulls=True).over(w_actor_event_id_by_event_at_r).alias("last_known_event_event_at"),
        F.rank().over(w_event_type_by_event_at_ties).alias("event_at_rank"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.min("event_at").alias("first_event_at"))
)

with_stats = (
    with_trends.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.first_event_at")
)

sequenced = (
    with_stats
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "event_id",
        "event_event_type",
        "event_actor",
        "event_at_dense_rank",
        "last_known_event_event_at",
        "first_event_at",
        F.round(F.col("event_at_dense_rank") / F.nullif(F.sum("event_at_dense_rank").over(w_event_at_dense_rank_event_at), F.lit(0)), 4).alias("event_at_dense_rank_share_of_event_at_dense_rank"),
        F.round(F.avg("event_at_rank").over(w_event_at_dense_rank_event_at), 2).alias("event_at_dense_rank_event_at_rank_avg_event_at_r"),
        F.round(F.cume_dist().over(w_event_actor_actor_by_last_kn), 3).alias("last_known_event_event_at_cume_dist"),
        F.ntile(3).over(w_event_actor_actor_by_last_kn).alias("last_known_event_event_at_tercile"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "event_id",
        F.col("event_at_dense_rank").alias("m_event_at_dense_rank"),
        F.col("last_known_event_event_at_tercile").alias("m_last_known_event_event_at_tercile"),
    )
    .unpivot(
        ids=["id", "event_id"],
        values=["m_event_at_dense_rank", "m_last_known_event_event_at_tercile"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "event_id", "metric")
)
