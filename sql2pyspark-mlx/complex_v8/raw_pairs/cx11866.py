from pyspark.sql import functions as F

recent = (
    df_policies
    .filter((F.col("holder_id").isNotNull()) & (F.col("premium") < 3465.4))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
    )
)

policy_rows = recent.select("holder_id", "premium", F.lit("policy").alias("origin"))

holder_side = (
    df_policyholders
    .filter(F.col("joined_on").between(F.lit("2024-02-04").cast("date"), F.lit("2025-05-01").cast("date")))
    .select(
        F.col("id").alias("holder_id"),
        F.col("risk_score").alias("premium"),
        F.lit("holder").alias("origin"),
    )
)

all_events = policy_rows.unionByName(holder_side)

result = (
    all_events
    .groupBy("origin")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("holder_id").alias("total_holder_id"),
    )
    .orderBy("origin")
)
