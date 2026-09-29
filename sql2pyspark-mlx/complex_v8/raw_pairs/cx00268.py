from pyspark.sql import functions as F

scoped_policys = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
    )
)

policy_side = (
    scoped_policys
    .filter(F.col("end_date").isNotNull())
    .select(
        F.col("id").alias("entity_id"),
        F.col("start_date").alias("event_date"),
        F.lit("policy").alias("source"),
    )
)

holder_rows = df_policyholders.select("id", "joined_on", F.lit("holder").alias("source"))

policy_side_2 = df_policies.select("holder_id", "start_date", F.lit("policy_3").alias("source"))

result = (
    policy_side
    .union(holder_rows)
    .union(policy_side_2)
)
