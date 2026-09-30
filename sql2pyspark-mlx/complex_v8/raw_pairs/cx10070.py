from pyspark.sql import functions as F

policy_side = (
    df_policies
    .select(
        F.lit("policy").alias("src"),
        "id",
        "holder_id",
        F.col("end_date").alias("event_date"),
    )
)

policy_side_2 = (
    df_policies
    .filter(F.col("premium") <= 3719.7)
    .select(
        F.lit("policy_2").alias("src"),
        "id",
        "holder_id",
        F.col("start_date").alias("event_date"),
    )
)

result = (
    policy_side
    .unionByName(policy_side_2)
    .distinct()
    .orderBy(F.col("src").desc())
)
