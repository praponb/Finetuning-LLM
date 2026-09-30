from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("claim_type").orderBy(F.col("approved_amount").desc(), F.col("id").desc())

filtered_claims = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("reported_at")).alias("days_since_reported_at"),
    )
)

annotated = (
    filtered_claims
    .filter(F.col("approved_amount") < 14993.0)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "status",
        "days_since_reported_at",
        F.least(F.col("approved_amount"), F.col("days_since_reported_at")).alias("least_approved_amount_days_since_reported_at"),
        F.lower(F.trim(F.col("claim_type"))).alias("claim_type_clean"),
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
    )
)

latest = (
    annotated
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

qualified = (
    latest
    .crossJoin(latest.agg(F.avg("days_since_reported_at").alias("overall_avg_days_since_reported_at")))
    .filter(F.col("days_since_reported_at") > F.col("overall_avg_days_since_reported_at"))
    .drop("overall_avg_days_since_reported_at")
)

annotated_2 = (
    qualified
    .select(
        "*",
        F.concat_ws(": ", F.col("status_or_unknown"), F.col("claim_type_clean")).alias("status_or_unknown_claim_type_clean_label"),
    )
)

result = (
    annotated_2
    .select("id", "approved_amount", "least_approved_amount_days_since_reported_at")
    .unpivot(
        ids=["id"],
        values=["approved_amount", "least_approved_amount_days_since_reported_at"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
