from pyspark.sql import functions as F

policy_side = (
    df_policies
    .select(
        "holder_id",
        F.col("premium").alias("value"),
        F.lit("policy").alias("record_type"),
    )
)

policy_side_2 = df_policies.select("id", "premium", F.lit("policy_2").alias("record_type"))

result = (
    policy_side
    .union(policy_side_2)
    .orderBy(F.col("holder_id").desc(), "value")
)
