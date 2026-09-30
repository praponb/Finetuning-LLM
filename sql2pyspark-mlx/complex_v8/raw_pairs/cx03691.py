from pyspark.sql import functions as F

prepared_policys = (
    df_policies
    .filter((F.col("product") == "HOME") & (F.col("status") == "ACTIVE"))
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "status",
        F.add_months(F.col("end_date"), 3).alias("end_date_plus_3m"),
    )
)

with_children = (
    prepared_policys.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.end_date_plus_3m",
        F.col("c.id").alias("claim_id"),
        "c.claim_date",
        F.col("c.status").alias("claim_status"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        "c.claim_type",
    )
)

result = (
    with_children
    .rollup("holder_id", "claim_id", "claim_type")
    .agg(F.max("claim_approved_amount").alias("max_claim_approved_amount"))
    .orderBy(
        F.col("holder_id").asc_nulls_last(),
        F.col("claim_id").asc_nulls_last(),
        F.col("claim_type").asc_nulls_last(),
    )
)
