from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("event_type_group").orderBy("event_at", "id")

scoped = df_ticket_events.filter(F.col("ticket_id").isNotNull())

enriched = (
    scoped
    .filter(F.col("event_at") >= F.lit("2025-01-31 13:00:00").cast("timestamp"))
    .select(
        "id",
        "event_at",
        "actor",
        F.when(F.col("event_type").isin("reopened", "created"), "group_a").when(F.col("event_type") == "resolved", "group_b").otherwise("other").alias("event_type_group"),
        F.substring(F.trim(F.col("event_type")), 1, 3).alias("event_type_prefix"),
    )
)

latest_per_key = (
    enriched
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

labeled = (
    latest_per_key
    .filter(F.month(F.col("event_at")).isin(1, 3))
    .select(
        "id",
        "actor",
        "event_type_prefix",
        F.coalesce(F.col("event_type_group"), F.lit("UNKNOWN")).alias("event_type_group_or_unknown"),
    )
)

result = (
    labeled
    .groupingSets(
        [["event_type_group_or_unknown", "actor"], ["event_type_group_or_unknown"]],
        "event_type_group_or_unknown", "actor",
    )
    .agg(
        F.countDistinct("actor").alias("distinct_actors"),
        F.count(F.when(~F.col("actor").isin("agent", "system"), 1)).alias("n_actor_not_in_agent_system"),
    )
    .orderBy(
        F.col("event_type_group_or_unknown").asc_nulls_last(),
        F.col("actor").asc_nulls_last(),
    )
)
