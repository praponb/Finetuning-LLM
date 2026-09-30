from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("ticket_id").orderBy("event_at_week", "id")

scoped_events = (
    df_ticket_events
    .filter(F.col("event_type").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.upper(F.trim(F.col("actor"))).alias("actor_clean"),
        F.date_trunc("week", F.col("event_at")).alias("event_at_week"),
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
    )
)

latest = (
    scoped_events
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

without_matches = (
    latest.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.actor") == "system") & (F.col("t.event_at") < F.lit("2025-03-02 09:00:00").cast("timestamp")), "left_semi")
    .filter(F.col("actor") == "agent")
)

result = (
    without_matches
    .groupingSets(
        [["actor", "event_type", "actor_or_unknown"], ["actor"]],
        "actor", "event_type", "actor_or_unknown",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("event_type").isin("escalated", "created"), 1).otherwise(0)).alias("n_event_type_in_escalated_crea"),
        F.countDistinct("ticket_id").alias("distinct_ticket_ids"),
    )
    .orderBy(
        F.col("actor").asc_nulls_last(),
        F.col("event_type").asc_nulls_last(),
        F.col("actor_or_unknown").asc_nulls_last(),
    )
)
