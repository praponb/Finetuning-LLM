from pyspark.sql import functions as F

holder_side = (
    df_policyholders
    .filter(F.col("risk_score") < 0.7)
    .select(F.col("id").alias("qty"))
)

policy_rows = (
    df_policies
    .filter((F.col("status").isin("ACTIVE", "LAPSED", "CANCELLED")) & (F.col("premium") < 3544.1))
    .select(F.col("id").alias("qty"))
)

result = (
    holder_side
    .intersectAll(policy_rows)
    .orderBy("qty")
)
