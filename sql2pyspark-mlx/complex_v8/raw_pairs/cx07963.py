from pyspark.sql import functions as F

recent_policys = (
    df_policies
    .filter(F.col("status") == "LAPSED")
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "status",
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
    )
)

policy_side = recent_policys.select(F.col("id").alias("qty"), F.col("status_slug").alias("label"))

claim_side = df_claims.select(F.col("id").alias("qty"), F.col("status").alias("label"))

result = (
    policy_side
    .subtract(claim_side)
    .orderBy("qty", "label")
)
