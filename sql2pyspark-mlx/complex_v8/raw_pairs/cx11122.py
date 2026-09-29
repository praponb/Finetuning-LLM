from pyspark.sql import functions as F

filtered = (
    df_ticket_events
    .filter(F.col("actor").isin("agent", "system"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.substring(F.trim(F.col("event_type")), 1, 4).alias("event_type_prefix"),
    )
)

tagged = filtered.withColumn("event_at_date", F.to_date(F.col("event_at")))

result = (
    tagged
    .cube("event_type", "ticket_id", "actor")
    .agg(
        F.sum(F.when(F.col("actor") == "customer", 1).otherwise(0)).alias("n_actor_customer"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("actor").isin("customer", "system"), 1).otherwise(0)).alias("n_actor_in_customer_system"),
    )
    .orderBy(
        F.col("event_type").asc_nulls_last(),
        F.col("ticket_id").asc_nulls_last(),
        F.col("actor").asc_nulls_last(),
    )
)
