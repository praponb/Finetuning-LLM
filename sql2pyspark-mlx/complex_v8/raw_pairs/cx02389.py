from pyspark.sql import functions as F

prepared_policys = (
    df_policies
    .filter((F.col("end_date").isNotNull()) & (F.col("product") == "TRAVEL"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("end_date")).alias("days_since_end_date"),
    )
)

policy_rows = (
    prepared_policys
    .filter((F.col("end_date") < F.lit("2026-09-19").cast("date")) & (F.col("status") != "ACTIVE"))
    .select(
        F.col("holder_id").alias("entity_id"),
        F.col("end_date").alias("day"),
        F.col("status").alias("label"),
    )
)

claim_rows = (
    df_claims
    .select(
        F.col("policy_id").alias("entity_id"),
        F.col("claim_date").alias("day"),
        F.col("claim_type").alias("label"),
    )
)

result = (
    policy_rows
    .unionByName(claim_rows)
    .distinct()
    .orderBy(F.col("entity_id").desc())
)
