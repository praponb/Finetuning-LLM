from pyspark.sql import functions as F

claim_rows = (
    df_claims
    .select(
        F.lit("claim").alias("record_type"),
        F.col("claim_type").alias("category"),
        "claim_date",
        "status",
    )
)

holder_side = (
    df_policyholders
    .filter(F.col("region").isNotNull())
    .select(
        F.lit("holder").alias("record_type"),
        "holder_name",
        "joined_on",
        "region",
    )
)

combined = (
    claim_rows
    .union(holder_side)
    .distinct()
)

result = (
    combined
    .groupBy("record_type")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("claim_date").alias("latest_claim_date"),
    )
    .orderBy("record_type")
)
