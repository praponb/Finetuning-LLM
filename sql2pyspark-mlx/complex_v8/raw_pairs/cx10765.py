from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("policy_id", "adjuster_id").orderBy(F.col("claimed_amount").desc(), F.col("id").desc())

scoped = (
    df_claims
    .filter((F.col("claim_type") != "flood") | (F.col("policy_id").isNotNull()))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "approved_amount",
        "reported_at",
    )
)

latest_per_key = (
    scoped
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

labeled = (
    latest_per_key
    .filter(F.col("adjuster_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "approved_amount",
        "reported_at",
        F.least(F.col("approved_amount"), F.col("claimed_amount")).alias("least_approved_amount_claimed_amount"),
        (F.col("reported_at") + F.expr("INTERVAL 1 DAYS")).alias("reported_at_plus_1d"),
    )
)

adjuster_id_stats = (
    labeled
    .groupBy("adjuster_id")
    .agg(F.percentile_approx("least_approved_amount_claimed_amount", 0.5).alias("median_least_approved_amount_claimed_amount"))
)

compared = (
    labeled.alias("s")
    .join(adjuster_id_stats.alias("g"), F.col("g.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .filter(F.col("s.least_approved_amount_claimed_amount") >= F.col("g.median_least_approved_amount_claimed_amount"))
    .select(
        "s.*",
        "g.median_least_approved_amount_claimed_amount",
        F.round(F.col("s.least_approved_amount_claimed_amount") - F.col("g.median_least_approved_amount_claimed_amount"), 2).alias("least_approved_amount_claimed_amount_minus_media"),
    )
)

result = (
    compared
    .filter(F.hour(F.col("reported_at")).between(10, 13))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "approved_amount",
        "reported_at",
        "least_approved_amount_claimed_amount",
        "reported_at_plus_1d",
        "median_least_approved_amount_claimed_amount",
        "least_approved_amount_claimed_amount_minus_media",
        F.round(F.col("approved_amount") * 1.1, 2).alias("approved_amount_adj"),
        F.date_trunc("day", F.col("reported_at")).alias("reported_at_day"),
    )
    .orderBy(
        F.col("least_approved_amount_claimed_amount_minus_media").asc_nulls_last(),
        F.col("approved_amount").desc_nulls_first(),
    )
)
