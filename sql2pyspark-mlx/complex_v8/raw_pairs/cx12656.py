from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("status", "claim_type").orderBy("status_len", "id")

recent = (
    df_claims
    .filter(F.col("status").isin("PAID", "IN_REVIEW"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        F.length(F.trim(F.col("status"))).alias("status_len"),
        F.date_add(F.col("claim_date"), 90).alias("claim_date_plus_90d"),
        F.when(F.col("claimed_amount") >= 28900.9, "large").when(F.col("claimed_amount") >= 11235.6, "medium").otherwise("small").alias("claimed_amount_tier"),
    )
)

screened = (
    recent.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.adjuster_id").isNotNull()), "left_anti")
)

latest_per_key = (
    screened
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest_per_key
    .rollup("claim_type", "policy_id")
    .agg(
        F.countDistinct("adjuster_id").alias("distinct_adjuster_ids"),
        F.sum(F.when(F.col("status") == "REJECTED", 1).otherwise(0)).alias("n_status_rejected"),
    )
    .orderBy(
        F.col("claim_type").asc_nulls_last(),
        F.col("policy_id").asc_nulls_last(),
    )
)
