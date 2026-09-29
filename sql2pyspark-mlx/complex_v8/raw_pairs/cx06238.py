from pyspark.sql import functions as F

holder_side = (
    df_policyholders
    .select(
        F.lit("holder").alias("record_type"),
        F.col("segment").alias("category"),
        "risk_score",
        F.col("joined_on").alias("event_date"),
    )
)

policy_rows = (
    df_policies
    .filter(F.col("holder_id") <= 5)
    .select(
        F.lit("policy").alias("record_type"),
        "product",
        "premium",
        "start_date",
    )
)

holder_rows = (
    df_policyholders
    .filter(~F.col("region").isin("Central", "North"))
    .select(
        F.lit("holder_3").alias("record_type"),
        "region",
        "risk_score",
        "joined_on",
    )
)

result = (
    holder_side
    .union(policy_rows)
    .union(holder_rows)
)
