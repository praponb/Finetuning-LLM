from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type_by_claimed_amount = Window.partitionBy("claim_type").orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_type_by_claimed_amount_2 = Window.partitionBy("claim_type").orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_reported_at_to_date = Window.partitionBy(F.to_date("reported_at"))

scoped_claims = (
    df_claims
    .filter((F.col("claim_date") < F.lit("2025-09-14").cast("date")) & (F.col("policy_id") <= 25))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.round(F.col("approved_amount") / F.nullif(F.col("claimed_amount"), F.lit(0)), 3).alias("approved_amount_per_claimed_amount"),
    )
)

ranked = (
    scoped_claims
    .select(
        "id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "approved_amount_per_claimed_amount",
        F.count(F.lit(1)).over(w_claim_type_by_claimed_amount).alias("running_count"),
        F.nth_value("claimed_amount", 2).over(w_claim_type_by_claimed_amount_2).alias("second_claimed_amount"),
        F.round(F.avg("approved_amount").over(w_reported_at_to_date), 2).alias("reported_at_to_date_avg_approved_amount"),
        F.nth_value("reported_at", 2).over(w_claim_type_by_claimed_amount_2).alias("second_reported_at"),
    )
)

result = (
    ranked
    .groupBy("second_reported_at", "adjuster_id")
    .pivot("claim_type", ["fire", "collision"])
    .agg(F.count("running_count"))
)
