from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_event_type = Window.partitionBy("ticket_id", "event_type")

valid = (
    df_ticket_events
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.when(F.col("event_type").isin("reopened", "escalated"), "group_a").when(F.col("event_type") == "created", "group_b").otherwise("other").alias("event_type_group"),
        F.when(F.col("actor").isin("system", "agent"), "group_a").when(F.col("actor") == "customer", "group_b").otherwise("other").alias("actor_group"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.round(F.col("days_since_event_at") - F.avg("days_since_event_at").over(w_ticket_id_event_type), 2).alias("days_since_event_at_vs_ticket_id_event_type_avg"),
        F.size(F.collect_set("actor").over(w_ticket_id_event_type)).alias("distinct_actor_in_ticket_id_event_type"),
        F.round(F.col("days_since_event_at") / F.nullif(F.sum("days_since_event_at").over(w_ticket_id_event_type), F.lit(0)), 4).alias("days_since_event_at_share_of_ticket_id_event_typ"),
    )
)

derived = (
    with_windows
    .filter(F.col("days_since_event_at_vs_ticket_id_event_type_avg") >= -140.0)
    .withColumn("event_at_week", F.date_trunc("week", F.col("event_at")))
)

result = (
    derived
    .rollup("distinct_actor_in_ticket_id_event_type", "ticket_id", "actor_group")
    .agg(
        F.grouping("distinct_actor_in_ticket_id_event_type").alias("is_total_distinct_actor_in_ticket_id_event_type"),
        F.array_sort(F.collect_set("actor")).alias("actor_list"),
    )
    .orderBy(
        F.col("distinct_actor_in_ticket_id_event_type").asc_nulls_last(),
        F.col("ticket_id").asc_nulls_last(),
        F.col("actor_group").asc_nulls_last(),
    )
)
