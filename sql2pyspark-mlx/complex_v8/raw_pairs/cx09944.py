from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("adjuster_id", "status").orderBy("approved_amount", "id")
w_dedup_2 = Window.partitionBy("claim_type", "policy_id").orderBy(F.col("approved_amount").desc(), F.col("id").desc())

valid = (
    df_claims
    .filter((F.col("claimed_amount") <= 34401.0) & (~F.col("claim_type").isin("flood", "theft")))
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
        F.concat_ws("-", F.col("status"), F.col("claim_type")).alias("status_claim_type_label"),
        (F.col("reported_at") + F.expr("INTERVAL 30 MINUTES")).alias("reported_at_plus_30m"),
    )
)

labeled = (
    valid
    .select(
        "*",
        F.coalesce(F.col("claimed_amount"), F.lit(0.0)).alias("claimed_amount_filled"),
    )
)

unique_rows = (
    labeled
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

latest = (
    unique_rows
    .withColumn("recency_rank", F.row_number().over(w_dedup_2))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest
    .filter(F.col("claim_date") >= F.lit("2024-08-01").cast("date"))
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
        "status_claim_type_label",
        "claimed_amount_filled",
    )
    .orderBy(F.col("status_claim_type_label").desc(), F.col("reported_at").asc())
)
