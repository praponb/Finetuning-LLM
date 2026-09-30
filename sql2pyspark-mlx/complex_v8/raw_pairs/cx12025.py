from pyspark.sql import functions as F

cleaned = (
    df_policies
    .filter(F.col("product").isin("HEALTH", "HOME"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
    )
)

policy_rows = (
    cleaned
    .filter(F.col("holder_id").isNotNull())
    .select("id", F.col("holder_id").alias("key_id"), F.lit("policy").alias("origin"))
)

policy_side = (
    df_policies
    .filter(F.col("start_date") >= F.lit("2024-10-22").cast("date"))
    .select("holder_id", "id", F.lit("policy_2").alias("origin"))
)

stacked = policy_rows.union(policy_side)

result = (
    stacked
    .groupBy("origin")
    .agg(F.count("*").alias("n_rows"), F.sum("id").alias("total_id"))
    .orderBy("origin")
)
