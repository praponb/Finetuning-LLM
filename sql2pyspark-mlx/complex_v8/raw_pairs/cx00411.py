from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type = Window.partitionBy("claim_type")
w_claim_type_reported_at_month = Window.partitionBy("claim_type", F.month("reported_at")).orderBy(F.col("claimed_amount").desc(), "id")
w_all_by_prev_claimed_amount = Window.orderBy(F.col("prev_claimed_amount").desc(), "id")
w_all_by_prev_claimed_amount_t = Window.orderBy(F.col("prev_claimed_amount").desc())
w_adjuster_id_reported_at_mont = Window.partitionBy("adjuster_id", F.month("reported_at")).orderBy("reported_at", "id").rowsBetween(-6, -1)

prepared_claims = (
    df_claims
    .filter(F.col("claimed_amount") >= 10673.3)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "reported_at",
        (F.col("approved_amount") + F.col("claimed_amount")).alias("approved_amount_plus_claimed_amount"),
    )
)

with_trends = (
    prepared_claims
    .select(
        "id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "reported_at",
        "approved_amount_plus_claimed_amount",
        F.round(F.avg("claimed_amount").over(w_claim_type), 2).alias("claim_type_avg_claimed_amount"),
        (F.col("claimed_amount") - F.lag("claimed_amount").over(w_claim_type_reported_at_month)).alias("claimed_amount_change"),
        F.lag("claimed_amount", 1, 0).over(w_claim_type_reported_at_month).alias("prev_claimed_amount"),
        F.round(F.cume_dist().over(w_claim_type_reported_at_month), 3).alias("claimed_amount_cume_dist"),
    )
)

sequenced = (
    with_trends
    .select(
        "id",
        "claim_type",
        "reported_at",
        "approved_amount_plus_claimed_amount",
        "claim_type_avg_claimed_amount",
        "claimed_amount_change",
        "prev_claimed_amount",
        F.rank().over(w_all_by_prev_claimed_amount).alias("prev_claimed_amount_rank"),
        F.rank().over(w_all_by_prev_claimed_amount_t).alias("prev_claimed_amount_rank_2"),
        F.max("prev_claimed_amount").over(w_adjuster_id_reported_at_mont).alias("prev_claimed_amount_max_6_prior"),
    )
)

result = (
    sequenced
    .cube("prev_claimed_amount_rank_2", "claim_type")
    .agg(
        F.grouping("prev_claimed_amount_rank_2").alias("is_total_prev_claimed_amount_rank_2"),
        F.round(F.stddev("prev_claimed_amount"), 2).alias("stddev_prev_claimed_amount"),
    )
    .select(
        "prev_claimed_amount_rank_2",
        F.coalesce(F.col("claim_type"), F.lit("ALL")).alias("claim_type"),
        "is_total_prev_claimed_amount_rank_2",
        "stddev_prev_claimed_amount",
    )
    .orderBy(
        F.col("prev_claimed_amount_rank_2").asc_nulls_last(),
        F.col("claim_type").asc_nulls_last(),
    )
)
