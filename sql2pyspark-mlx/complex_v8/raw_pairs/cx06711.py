from pyspark.sql import functions as F

policy_rows = (
    df_policies
    .select(
        F.lit("policy").alias("src"),
        F.col("holder_id").alias("entity_id"),
        F.col("end_date").alias("event_date"),
        "id",
    )
)

policy_side = (
    df_policies
    .filter((F.col("status") == "ACTIVE") & (F.col("start_date").between(F.lit("2024-03-15").cast("date"), F.lit("2025-05-28").cast("date"))))
    .select(
        F.lit("policy_2").alias("src"),
        "holder_id",
        "end_date",
        "id",
    )
)

combined = policy_rows.union(policy_side)

result = (
    combined
    .groupBy("src")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("entity_id").alias("total_entity_id"),
        F.max("event_date").alias("latest_event_date"),
    )
    .orderBy("src")
)
