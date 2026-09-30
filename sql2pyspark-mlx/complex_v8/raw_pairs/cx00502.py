from pyspark.sql import functions as F

recent = (
    df_policies
    .filter(F.col("premium") < 4417.0)
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

joined = (
    recent.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.joined_on").between(F.lit("2024-06-18").cast("date"), F.lit("2025-05-01").cast("date"))), "inner")
    .filter(F.col("p.segment") == "SME")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.start_date",
        "s.end_date",
        "s.status",
        F.col("p.region").alias("holder_region"),
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.segment").alias("holder_segment"),
    )
)

screened = (
    joined.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.adjuster_id").isNotNull()) & (~F.col("c.status").isin("OPEN", "IN_REVIEW")), "left_anti")
)

result = (
    screened
    .groupBy("holder_region")
    .pivot("status", ["CANCELLED", "EXPIRED"])
    .agg(F.avg("premium"))
)
