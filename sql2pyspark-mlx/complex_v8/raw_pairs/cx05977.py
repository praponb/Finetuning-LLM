from pyspark.sql import functions as F

scoped = (
    df_policies
    .filter((~F.col("status").isin("EXPIRED", "CANCELLED")) & (F.col("end_date").between(F.lit("2025-06-08").cast("date"), F.lit("2026-11-25").cast("date"))))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
    )
)

policy_rows = (
    scoped
    .filter(F.col("status") == "LAPSED")
    .select("holder_id", F.col("premium").alias("amount"), "start_date")
)

holder_side = (
    df_policyholders
    .filter(F.upper(F.trim(F.col("holder_name"))).like("G%"))
    .select(
        F.col("id").alias("holder_id"),
        F.col("risk_score").alias("amount"),
        F.col("joined_on").alias("start_date"),
    )
)

policy_side = df_policies.select("holder_id", F.col("premium").alias("amount"), "start_date")

result = (
    policy_rows
    .union(holder_side)
    .union(policy_side)
    .orderBy(F.col("start_date").desc(), "holder_id")
)
