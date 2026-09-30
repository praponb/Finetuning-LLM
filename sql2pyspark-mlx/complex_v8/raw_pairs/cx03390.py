from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_ticket_id_by_days = Window.partitionBy("event_type", "ticket_id").orderBy(F.col("days_since_event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_ticket_id_by_days_2 = Window.partitionBy("event_type", "ticket_id").orderBy(F.col("days_since_event_at").desc(), "id")
w_actor_event_type_len_by_days = Window.partitionBy("actor", "event_type_len").orderBy(F.col("days_since_event_at_2").asc_nulls_last(), "id")
w_event_type = Window.partitionBy("event_type")
w_actor_event_type = Window.partitionBy("actor", "event_type")

prepared = (
    df_ticket_events
    .filter((F.col("event_type") == "escalated") & (F.col("ticket_id") >= 54))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at")).alias("days_since_event_at_2"),
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
    )
)

with_trends = (
    prepared
    .withColumn("first_event_at", F.first("event_at").over(w_event_type_ticket_id_by_days))
    .withColumn("next_days_since_event_at", F.lead("days_since_event_at").over(w_event_type_ticket_id_by_days_2))
)

ranked = (
    with_trends
    .withColumn("days_since_event_at_2_tercile", F.ntile(3).over(w_actor_event_type_len_by_days))
    .withColumn("distinct_actor_in_event_type", F.size(F.collect_set("actor").over(w_event_type)))
)

windowed = (
    ranked
    .withColumn("event_type_len_share_of_actor_event_type", F.round(F.col("event_type_len") / F.nullif(F.sum("event_type_len").over(w_actor_event_type), F.lit(0)), 4))
    .withColumn("rows_in_actor_event_type", F.count(F.lit(1)).over(w_actor_event_type))
)

result = (
    windowed
    .groupBy("event_type_len", "ticket_id")
    .pivot("event_type", ["reopened", "created", "assigned", "resolved"])
    .agg(F.min("next_days_since_event_at"))
)
