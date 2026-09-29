from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("adjuster_id", "policy_id").orderBy(F.col("reported_at").desc(), F.col("id").desc())
w_policy_id_by_claim_date_ties = Window.partitionBy("policy_id").orderBy(F.col("claim_date").desc())
w_policy_id_by_reported_at_ful = Window.partitionBy("policy_id").orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_claims = (
    df_claims
    .filter(F.col("adjuster_id").isNotNull())
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
        F.month(F.col("claim_date")).alias("claim_date_month"),
    )
)

unique_rows = (
    filtered_claims
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_trends = (
    unique_rows
    .select(
        "*",
        F.round(F.cume_dist().over(w_policy_id_by_claim_date_ties), 3).alias("claim_date_cume_dist"),
        F.last("claimed_amount").over(w_policy_id_by_reported_at_ful).alias("last_claimed_amount"),
    )
)

result = (
    with_trends
    .groupBy("claim_date_month", "policy_id")
    .pivot("claim_type", ["collision", "medical"])
    .agg(F.count("claim_date_cume_dist"))
    .orderBy("claim_date_month", "policy_id")
)
