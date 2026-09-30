from pyspark.sql import functions as F

claim_side = df_claims.select("id", "reported_at", "claim_date")

claim_rows = df_claims.select("adjuster_id", "reported_at", "claim_date")

result = (
    claim_side
    .union(claim_rows)
    .distinct()
)
