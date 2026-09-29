from pyspark.sql import functions as F

claim_side = (
    df_claims
    .select(
        F.col("claim_type").alias("category"),
        F.col("status").alias("category_2"),
        "claimed_amount",
    )
)

policy_rows = (
    df_policies
    .filter(F.col("status").isNotNull())
    .select("status", "product", "premium")
)

result = (
    claim_side
    .union(policy_rows)
    .distinct()
    .orderBy("claimed_amount", "category_2")
)
