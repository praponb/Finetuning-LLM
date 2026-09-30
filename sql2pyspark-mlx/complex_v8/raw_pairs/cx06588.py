from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("claim_type", "policy_status").orderBy(F.col("approved_amount").desc(), F.col("id").desc())

valid_claims = df_claims.filter(F.col("policy_id").isNotNull())

combined_details_adjuster = (
    valid_claims.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        F.col("a.level").alias("adjuster_level"),
        F.col("p.status").alias("policy_status"),
    )
)

latest_per_key = (
    combined_details_adjuster
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

enriched = (
    latest_per_key
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "adjuster_level",
        "policy_status",
        F.concat_ws(": ", F.col("status"), F.col("policy_status")).alias("status_policy_status_label"),
        F.coalesce(F.col("adjuster_level"), F.lit(0)).alias("adjuster_level_filled"),
        (F.floor(F.col("approved_amount") / 10000) * 10000).alias("approved_amount_bucket"),
    )
)

result = (
    enriched
    .rollup("adjuster_level_filled", "policy_id")
    .agg(F.grouping_id().alias("gid"), F.count("*").alias("n_rows"))
    .orderBy(
        F.col("adjuster_level_filled").asc_nulls_last(),
        F.col("policy_id").asc_nulls_last(),
    )
)
