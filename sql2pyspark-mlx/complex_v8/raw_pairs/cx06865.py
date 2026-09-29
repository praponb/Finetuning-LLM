from pyspark.sql import functions as F

claim_side = (
    df_claims
    .select(
        F.lit("claim").alias("record_type"),
        F.col("id").alias("entity_id"),
        "status",
    )
)

holder_side = df_policyholders.select(F.lit("holder").alias("record_type"), "id", "holder_name")

result = claim_side.union(holder_side)
