from pyspark.sql import functions as F

claim_side = (
    df_claims
    .filter(F.col("claim_date") < F.lit("2024-12-02").cast("date"))
    .select(
        "status",
        F.col("claim_date").alias("day"),
        F.col("claimed_amount").alias("metric"),
    )
)

policy_rows = (
    df_policies
    .select(
        F.col("product").alias("status"),
        F.col("start_date").alias("day"),
        F.col("premium").alias("metric"),
    )
)

result = (
    claim_side
    .unionByName(policy_rows)
    .orderBy(F.col("day").desc(), "metric")
)
