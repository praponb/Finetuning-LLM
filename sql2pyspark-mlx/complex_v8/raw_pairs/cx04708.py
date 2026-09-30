from pyspark.sql import functions as F

base = (
    df_policyholders
    .filter((F.col("region").isin("Central", "East")) | (F.upper(F.trim(F.col("holder_name"))).like("L%")))
    .select(
        "id",
        "risk_score",
        "joined_on",
        "segment",
        F.date_format(F.col("joined_on"), "yyyy-MM-dd").alias("joined_on_label"),
    )
)

holder_side = (
    base
    .filter(F.col("joined_on").between(F.lit("2024-05-23").cast("date"), F.lit("2025-05-01").cast("date")))
    .select(F.col("segment").alias("label"), F.col("risk_score").alias("amount"))
)

policy_side = df_policies.select("product", "premium")

all_events = holder_side.union(policy_side)

result = (
    all_events
    .groupBy("label")
    .agg(F.count("*").alias("n_rows"), F.sum("amount").alias("total_amount"))
    .orderBy("label")
)
