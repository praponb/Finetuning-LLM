from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status = Window.partitionBy("status")
w_all_by_claim_date_full = Window.orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base = (
    df_claims
    .filter(F.col("reported_at") < F.lit("2025-02-24 13:00:00").cast("timestamp"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
    )
)

windowed = (
    base
    .select(
        "id",
        "adjuster_id",
        "approved_amount",
        "status",
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_status), 2).alias("approved_amount_vs_status_avg"),
        F.last("claim_date").over(w_all_by_claim_date_full).alias("last_claim_date"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(F.max("claim_date").alias("last_claim_date_2"))
)

profiled = (
    windowed.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select("s.*", "c.last_claim_date_2")
)

enriched = (
    profiled
    .select(
        "*",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
    )
)

result = (
    enriched
    .select("status", "status_or_unknown", "last_claim_date")
    .distinct()
    .orderBy(
        F.col("last_claim_date").asc_nulls_last(),
        F.col("status_or_unknown").asc(),
    )
)
