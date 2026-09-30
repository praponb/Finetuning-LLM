from pyspark.sql import functions as F

cleaned_claims = (
    df_claims
    .filter(F.col("adjuster_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "status",
        "reported_at",
    )
)

claim_rows = cleaned_claims.select("id")

policy_side = (
    df_policies
    .filter(F.col("holder_id").isNotNull())
    .select("id")
)

result = (
    claim_rows
    .subtract(policy_side)
    .orderBy("id")
)
