from pyspark.sql import functions as F

filtered_policys = (
    df_policies
    .filter((F.col("start_date") >= F.lit("2024-08-27").cast("date")) & (~F.col("product").isin("TRAVEL", "LIFE")))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "status",
    )
)

policy_rows = (
    filtered_policys
    .filter((~F.col("product").isin("LIFE", "TRAVEL")) & (F.col("start_date").isNotNull()))
    .select("status")
)

claim_rows = df_claims.select(F.col("claim_type").alias("status"))

result = (
    policy_rows
    .subtract(claim_rows)
    .orderBy("status")
)
