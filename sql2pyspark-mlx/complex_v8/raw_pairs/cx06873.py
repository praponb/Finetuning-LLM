from pyspark.sql import functions as F

scoped = (
    df_policyholders
    .filter(F.col("risk_score") < 0.8)
    .select(
        "id",
        "holder_name",
        "risk_score",
        "joined_on",
        "segment",
        F.quarter(F.col("joined_on")).alias("joined_on_quarter"),
    )
)

holder_side = (
    scoped
    .filter((F.col("risk_score") > 0.2) & (F.col("segment") == "SME"))
    .select(F.col("joined_on_quarter").alias("qty"))
)

claim_side = (
    df_claims
    .filter(F.col("claimed_amount") >= 10673.3)
    .select(F.col("adjuster_id").alias("qty"))
)

result = (
    holder_side
    .intersect(claim_side)
    .orderBy("qty")
)
