from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type = Window.partitionBy("claim_type")
w_claim_type_policy_id_by_clai = Window.partitionBy("claim_type", "policy_id").orderBy("claimed_amount")

recent = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        (F.col("approved_amount") - F.col("claimed_amount")).alias("approved_amount_minus_claimed_amount"),
    )
)

ranked = (
    recent
    .select(
        "*",
        F.max("claimed_amount").over(w_claim_type).alias("claim_type_max_claimed_amount"),
        F.rank().over(w_claim_type_policy_id_by_clai).alias("claimed_amount_rank"),
        F.round(F.avg("approved_amount").over(w_claim_type), 2).alias("claim_type_avg_approved_amount"),
        F.size(F.collect_set("policy_id").over(w_claim_type)).alias("distinct_policy_id_in_claim_type"),
    )
)

labeled = (
    ranked
    .filter(F.col("adjuster_id") <= 3)
    .select(
        "*",
        (F.floor(F.col("claimed_amount") / 10000) * 10000).alias("claimed_amount_bucket"),
        F.when(F.col("claim_type").isin("fire", "medical"), "group_a").when(F.col("claim_type") == "flood", "group_b").otherwise("other").alias("claim_type_group"),
        F.greatest(F.col("claimed_amount"), F.col("approved_amount")).alias("greatest_claimed_amount_approved_amount"),
    )
)

result = (
    labeled
    .groupBy("distinct_policy_id_in_claim_type", "adjuster_id")
    .pivot("claim_type", ["collision", "flood", "theft"])
    .agg(F.max("claim_type_max_claimed_amount").alias("max_v"), F.sum("claim_type_max_claimed_amount").alias("sum_v"))
)
