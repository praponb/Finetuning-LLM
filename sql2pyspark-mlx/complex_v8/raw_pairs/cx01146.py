from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_reported_at = Window.orderBy(F.col("reported_at").desc(), "id")
w_reported_at_year_by_approved = Window.partitionBy(F.year("reported_at")).orderBy("approved_amount", "id")
w_adjuster_id_policy_id = Window.partitionBy("adjuster_id", "policy_id")

base = (
    df_claims
    .filter((F.col("claim_date") >= F.lit("2024-04-15").cast("date")) & (F.col("policy_id") <= 5))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.abs(F.col("claimed_amount") - 29359.4).alias("claimed_amount_distance"),
    )
)

with_trends = (
    base
    .select(
        "*",
        F.ntile(3).over(w_all_by_reported_at).alias("reported_at_tercile"),
        F.round(F.cume_dist().over(w_reported_at_year_by_approved), 3).alias("approved_amount_cume_dist"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_adjuster_id_policy_id), F.lit(0)), 4).alias("approved_amount_share_of_adjuster_id_policy_id"),
        (F.col("claimed_amount") - F.lag("claimed_amount").over(w_all_by_reported_at)).alias("claimed_amount_change"),
    )
)

result = (
    with_trends
    .cube("claim_type", "adjuster_id")
    .agg(
        F.grouping("claim_type").alias("is_total_claim_type"),
        F.max("approved_amount_share_of_adjuster_id_policy_id").alias("max_approved_amount_share_of_adjuster_id_policy_"),
        F.round(F.avg("approved_amount"), 2).alias("avg_approved_amount"),
    )
    .select(
        F.coalesce(F.col("claim_type"), F.lit("ALL")).alias("claim_type"),
        "adjuster_id",
        "is_total_claim_type",
        "max_approved_amount_share_of_adjuster_id_policy_",
        "avg_approved_amount",
    )
    .orderBy(
        F.col("claim_type").asc_nulls_last(),
        F.col("adjuster_id").asc_nulls_last(),
    )
)
