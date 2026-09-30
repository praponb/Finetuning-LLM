from pyspark.sql import functions as F

policy_side = (
    df_policies
    .filter((F.col("status").isin("CANCELLED", "LAPSED", "EXPIRED")) & (F.col("holder_id").isNotNull()))
    .select(
        F.col("holder_id").alias("qty"),
        "premium",
        F.lit("policy").alias("origin"),
    )
)

holder_side = (
    df_policyholders
    .filter(F.col("holder_name").isNotNull())
    .select("id", "risk_score", F.lit("holder").alias("origin"))
)

unioned = (
    policy_side
    .union(holder_side)
    .distinct()
)

result = (
    unioned
    .groupBy("origin")
    .agg(F.count("*").alias("n_rows"), F.sum("qty").alias("total_qty"))
    .orderBy("origin")
)
