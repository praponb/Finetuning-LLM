from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_reported_at = Window.orderBy(F.col("reported_at").desc_nulls_first(), "id")
w_policy_id = Window.partitionBy("policy_id")
w_dedup = Window.partitionBy("adjuster_id", "status").orderBy(F.col("claim_date").desc(), F.col("id").desc())

filtered = (
    df_claims
    .filter((F.col("status").isNotNull()) & (F.col("approved_amount").between(0.0, 38386.6)))
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
    )
)

ranked = (
    filtered
    .select(
        "*",
        F.ntile(4).over(w_all_by_reported_at).alias("reported_at_quartile"),
        F.max("approved_amount").over(w_policy_id).alias("policy_id_max_approved_amount"),
        F.lag("claim_date", 3).over(w_all_by_reported_at).alias("prev3_claim_date"),
        F.min("approved_amount").over(w_policy_id).alias("policy_id_min_approved_amount"),
    )
)

latest = (
    ranked
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

line_level_claims = (
    latest.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.approved_amount",
        "s.reported_at",
        "s.policy_id_max_approved_amount",
        "s.prev3_claim_date",
        F.col("c.id").alias("claim_id"),
        F.col("c.claim_type").alias("claim_claim_type"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
    )
)

result = (
    line_level_claims
    .filter(F.col("claimed_amount") > 13635.1)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        "prev3_claim_date",
        "claim_id",
        (F.col("claimed_amount") - F.col("approved_amount")).alias("claimed_amount_minus_approved_amount"),
        F.when(F.col("approved_amount") >= 14595.6, "A").when(F.col("approved_amount") >= 2569.2, "B").otherwise("C").alias("approved_amount_tier"),
    )
    .orderBy(
        F.col("claimed_amount_minus_approved_amount").asc(),
        F.col("claim_id").desc(),
        "id",
    )
    .limit(10)
)
