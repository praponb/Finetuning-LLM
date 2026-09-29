from pyspark.sql import functions as F

claim_rows = (
    df_claims
    .select(
        F.lit("claim").alias("record_type"),
        F.col("status").alias("category"),
        "claim_date",
        F.col("claim_type").alias("name"),
    )
)

policy_side = (
    df_policies
    .filter((F.col("end_date").isNotNull()) & (F.col("holder_id") >= 10))
    .select(
        F.lit("policy").alias("record_type"),
        "status",
        "end_date",
        "product",
    )
)

result = (
    claim_rows
    .union(policy_side)
    .orderBy(F.col("claim_date").desc())
)
