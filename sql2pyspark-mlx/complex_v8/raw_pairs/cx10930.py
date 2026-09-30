from pyspark.sql import functions as F

policy_side = df_policies.select("holder_id")

holder_rows = (
    df_policyholders
    .filter((F.col("segment") == "SME") & (F.col("risk_score") > 0.2))
    .select("id")
)

result = (
    policy_side
    .exceptAll(holder_rows)
    .orderBy(F.col("holder_id").desc())
)
