from pyspark.sql import functions as F

cleaned_policys = (
    df_policies
    .filter((F.col("product").isin("LIFE", "TRAVEL")) & (F.col("holder_id") <= 5))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

policy_rows = (
    cleaned_policys
    .filter((F.col("premium") > 1051.9) & (F.col("start_date").between(F.lit("2024-03-15").cast("date"), F.lit("2025-05-30").cast("date"))))
    .select(
        F.col("holder_id").alias("entity_id"),
        "id",
        F.col("premium").alias("value"),
        F.lit("policy").alias("src"),
    )
)

claim_side = (
    df_claims
    .filter(F.col("adjuster_id") <= 3)
    .select(
        "id",
        "policy_id",
        "approved_amount",
        F.lit("claim").alias("src"),
    )
)

stacked = policy_rows.union(claim_side)

result = (
    stacked
    .groupBy("src")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("entity_id").alias("total_entity_id"),
    )
    .orderBy("src")
)
