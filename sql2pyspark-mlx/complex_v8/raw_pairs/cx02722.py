from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type_status_by_claimed = Window.partitionBy("claim_type", "status").orderBy("claimed_amount_distance", "id").rowsBetween(-2, Window.currentRow)
w_adjuster_id = Window.partitionBy("adjuster_id")

recent = (
    df_claims
    .filter((F.col("adjuster_id").isNotNull()) | (F.col("claimed_amount") > 18924.4))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        F.coalesce(F.col("claim_type"), F.lit("UNKNOWN")).alias("claim_type_or_unknown"),
    )
)

annotated = (
    recent
    .filter(F.col("claim_date") >= F.lit("2024-07-08").cast("date"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "status",
        F.greatest(F.col("approved_amount"), F.col("claimed_amount")).alias("greatest_approved_amount_claimed_amount"),
        F.abs(F.col("claimed_amount") - 20436.5).alias("claimed_amount_distance"),
    )
)

with_trends = (
    annotated
    .select(
        "*",
        F.max("approved_amount").over(w_claim_type_status_by_claimed).alias("approved_amount_max_3"),
        F.size(F.collect_set("claim_type").over(w_adjuster_id)).alias("distinct_claim_type_in_adjuster_id"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_adjuster_id), 2).alias("approved_amount_vs_adjuster_id_avg"),
    )
)

result = (
    with_trends
    .filter(F.col("claim_type").isin("collision", "fire", "delay"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "approved_amount",
        "status",
        "greatest_approved_amount_claimed_amount",
        "approved_amount_max_3",
        "distinct_claim_type_in_adjuster_id",
        "approved_amount_vs_adjuster_id_avg",
    )
    .orderBy(F.col("approved_amount").asc())
)
