from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_event_type_group = Window.partitionBy("event_type", "event_type_group").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_event_type_group_b = Window.partitionBy("ticket_id", "event_type_group").orderBy("event_at")
w_actor_event_type_by_event_at = Window.partitionBy("actor", "event_type").orderBy("event_at")
w_ticket_id_event_type_group_b_2 = Window.partitionBy("ticket_id", "event_type_group").orderBy("event_at", "id")

cleaned = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "yyyy-MM").alias("event_at_label"),
        F.when(F.col("event_type").isin("escalated", "resolved"), "group_a").when(F.col("event_type") == "reopened", "group_b").otherwise("other").alias("event_type_group"),
        F.concat(F.col("actor"), F.lit("#"), F.col("ticket_id").cast("string")).alias("actor_ref"),
    )
)

sequenced = (
    cleaned
    .select(
        "*",
        F.last("event_at").over(w_event_type_event_type_group).alias("last_event_at"),
        F.dense_rank().over(w_ticket_id_event_type_group_b).alias("event_at_dense_rank"),
        F.round(F.percent_rank().over(w_actor_event_type_by_event_at), 3).alias("event_at_percent_rank"),
        F.lead("event_at").over(w_ticket_id_event_type_group_b_2).alias("next_event_at"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at_2"),
        F.count("*").alias("event_count"),
    )
)

profiled = (
    sequenced.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.last_event_at_2",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

result = (
    profiled
    .select("id", "event_at_dense_rank", "event_count")
    .unpivot(
        ids=["id"],
        values=["event_at_dense_rank", "event_count"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
