from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_by_reported_at_ties = Window.partitionBy("status").orderBy("reported_at")
w_adjuster_id_policy_id = Window.partitionBy("adjuster_id", "policy_id")

filtered_claims = (
    df_claims
    .filter(F.col("claimed_amount") < 31715.7)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
    )
)

claim_type_stats = (
    filtered_claims
    .groupBy("claim_type")
    .agg(F.avg("claimed_amount").alias("avg_claimed_amount"))
)

compared = (
    filtered_claims.alias("s")
    .join(claim_type_stats.alias("g"), F.col("g.claim_type") == F.col("s.claim_type"), "inner")
    .filter(F.col("s.claimed_amount") > F.col("g.avg_claimed_amount"))
    .select(
        "s.*",
        "g.avg_claimed_amount",
        F.round(F.col("s.claimed_amount") - F.col("g.avg_claimed_amount"), 2).alias("claimed_amount_minus_avg"),
    )
)

sequenced = (
    compared
    .select(
        "id",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        "claimed_amount_minus_avg",
        F.dense_rank().over(w_status_by_reported_at_ties).alias("reported_at_dense_rank"),
        F.size(F.collect_set("status").over(w_adjuster_id_policy_id)).alias("distinct_status_in_adjuster_id_policy_id"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "claim_type",
        "reported_at",
        "reported_at_dense_rank",
        F.round(F.col("reported_at_dense_rank") / F.nullif(F.col("claimed_amount"), F.lit(0)), 3).alias("reported_at_dense_rank_per_claimed_amount"),
    )
    .orderBy(F.col("reported_at_dense_rank").desc_nulls_first(), F.col("id").desc())
    .limit(10)
)
