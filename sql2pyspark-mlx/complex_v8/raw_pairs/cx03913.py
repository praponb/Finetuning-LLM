from pyspark.sql import functions as F

claim_rows = (
    df_claims
    .filter(F.col("adjuster_id") <= 6)
    .select("policy_id", "reported_at")
)

claim_rows_2 = df_claims.select("adjuster_id", "reported_at")

claim_side = (
    df_claims
    .filter((F.to_date(F.col("reported_at")) < F.lit("2025-03-02").cast("date")) & (F.col("status").isin("OPEN", "IN_REVIEW", "PAID")))
    .select("id", "reported_at")
)

result = (
    claim_rows
    .union(claim_rows_2)
    .union(claim_side)
    .orderBy("reported_at")
)
