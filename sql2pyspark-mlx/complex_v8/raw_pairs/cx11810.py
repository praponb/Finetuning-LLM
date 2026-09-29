from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_claim_date = Window.orderBy("claim_date", "id")
w_claim_date_month_by_claimed = Window.partitionBy(F.month("claim_date")).orderBy(F.col("claimed_amount").desc(), "id")
w_all_by_claim_date_full = Window.orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid = (
    df_claims
    .filter((~F.col("claim_type").isin("delay", "fire")) & (F.col("approved_amount") >= 2349.3))
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
        F.upper(F.trim(F.col("claim_type"))).alias("claim_type_clean"),
    )
)

ranked = (
    valid
    .select(
        "*",
        F.dense_rank().over(w_all_by_claim_date).alias("claim_date_dense_rank"),
        F.ntile(3).over(w_claim_date_month_by_claimed).alias("claimed_amount_tercile"),
        F.lead("reported_at").over(w_claim_date_month_by_claimed).alias("next_reported_at"),
        F.first("approved_amount").over(w_all_by_claim_date_full).alias("first_approved_amount"),
        F.dense_rank().over(w_claim_date_month_by_claimed).alias("claimed_amount_dense_rank"),
    )
)

tagged = (
    ranked
    .filter(F.col("approved_amount") < 24718.5)
    .withColumn("claimed_amount_tercile_per_approved_amount", F.round(F.col("claimed_amount_tercile") / F.nullif(F.col("approved_amount"), F.lit(0)), 3))
)

line_level = (
    tagged.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.claimed_amount").between(13898.1, 43724.9)), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.reported_at",
        "s.claim_date_dense_rank",
        "s.first_approved_amount",
        "s.claimed_amount_tercile_per_approved_amount",
        F.col("c.id").alias("claim_id"),
        F.col("c.policy_id").alias("claim_policy_id"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
    )
)

result = (
    line_level
    .rollup("claim_policy_id", "adjuster_id", "policy_id")
    .agg(
        F.percentile_approx("claimed_amount_tercile_per_approved_amount", 0.5).alias("median_claimed_amount_tercile_per_approved_amoun"),
        F.round(F.stddev("first_approved_amount"), 2).alias("stddev_first_approved_amount"),
    )
)
