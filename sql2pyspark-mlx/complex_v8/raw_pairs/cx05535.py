from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_by_claimed_amoun = Window.partitionBy("adjuster_id").orderBy("claimed_amount", "id")
w_all_by_claimed_amount_full = Window.orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid = (
    df_claims
    .filter((F.col("reported_at") < F.lit("2025-02-24 07:00:00").cast("timestamp")) | (F.col("claim_date").between(F.lit("2024-03-24").cast("date"), F.lit("2025-12-03").cast("date"))))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        F.initcap(F.trim(F.col("status"))).alias("status_clean"),
    )
)

with_trends = (
    valid
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "status",
        "reported_at",
        "status_clean",
        F.lead("claim_date", 3).over(w_adjuster_id_by_claimed_amoun).alias("next3_claim_date"),
        F.nth_value("reported_at", 2).over(w_all_by_claimed_amount_full).alias("second_reported_at"),
    )
)

annotated = (
    with_trends
    .filter(F.col("claim_type") == "collision")
    .select(
        "*",
        F.when(F.col("status").isin("OPEN", "IN_REVIEW"), "group_a").when(F.col("status") == "APPROVED", "group_b").otherwise("other").alias("status_group"),
        F.when(F.col("claim_type").isin("fire", "collision"), "group_a").when(F.col("claim_type") == "delay", "group_b").otherwise("other").alias("claim_type_group"),
        F.datediff(F.col("claim_date"), F.col("next3_claim_date")).alias("days_next3_claim_date_to_claim_date"),
    )
)

result = (
    annotated
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "status",
        "reported_at",
        "status_clean",
        "next3_claim_date",
        "second_reported_at",
        "status_group",
        "days_next3_claim_date_to_claim_date",
        F.lower(F.trim(F.col("claim_type_group"))).alias("claim_type_group_clean"),
    )
    .orderBy(F.col("status_group").desc(), F.col("next3_claim_date").desc())
)
