from pyspark.sql import functions as F

base_policys = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.trunc(F.col("end_date"), "MM").alias("end_date_month_start"),
        F.concat(F.col("status"), F.lit("#"), F.col("holder_id").cast("string")).alias("status_ref"),
        F.when(F.col("premium") >= 3817.5, "large").when(F.col("premium") >= 1132.1, "medium").otherwise("small").alias("premium_tier"),
    )
)

qualified = (
    base_policys.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claim_date") < F.lit("2024-12-05").cast("date")) & (F.col("c.approved_amount") > 0.0), "left_semi")
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.sum("approved_amount").alias("total_claim_approved_amount"),
        F.max("reported_at").alias("last_claim_reported_at"),
    )
)

with_counts = (
    qualified.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select("s.*", "c.total_claim_approved_amount", "c.last_claim_reported_at")
)

result = (
    with_counts
    .select("id", "total_claim_approved_amount", "premium")
    .unpivot(
        ids=["id"],
        values=["total_claim_approved_amount", "premium"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "metric")
)
