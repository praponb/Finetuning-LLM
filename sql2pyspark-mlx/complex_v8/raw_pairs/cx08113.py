from pyspark.sql import functions as F

holder_rows = (
    df_policyholders
    .select(
        F.lit("holder").alias("record_type"),
        "id",
        F.col("risk_score").alias("metric"),
        "holder_name",
    )
)

policy_rows = (
    df_policies
    .select(
        F.lit("policy").alias("record_type"),
        F.col("holder_id").alias("id"),
        F.col("premium").alias("metric"),
        F.col("status").alias("holder_name"),
    )
)

combined = holder_rows.unionByName(policy_rows)

result = (
    combined
    .groupBy("record_type")
    .agg(F.count("*").alias("n_rows"), F.sum("metric").alias("total_metric"))
    .orderBy("record_type")
)
