from pyspark.sql import functions as F

holder_side = (
    df_policyholders
    .filter((F.col("risk_score") < 0.8) & (F.col("region") != "South"))
    .select(
        F.lit("holder").alias("record_type"),
        F.col("id").alias("entity_id"),
        "risk_score",
        F.col("joined_on").alias("event_date"),
    )
)

policy_side = (
    df_policies
    .filter((F.col("status").isin("CANCELLED", "EXPIRED", "LAPSED")) & (F.col("end_date") < F.lit("2026-09-23").cast("date")))
    .select(
        F.lit("policy").alias("record_type"),
        "holder_id",
        "premium",
        "end_date",
    )
)

combined = holder_side.union(policy_side)

result = (
    combined
    .groupBy("record_type")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("entity_id").alias("total_entity_id"),
        F.max("event_date").alias("latest_event_date"),
    )
    .orderBy("record_type")
)
