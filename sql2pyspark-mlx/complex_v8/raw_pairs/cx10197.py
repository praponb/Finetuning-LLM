from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("policy_id", "adjuster_id").orderBy(F.col("claimed_amount_minus_avg").desc(), F.col("id").desc())

prepared = (
    df_claims
    .filter((F.col("claimed_amount") <= 34763.4) & (F.col("policy_id") <= 28))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
        (F.col("reported_at") + F.expr("INTERVAL 1 DAYS")).alias("reported_at_plus_1d"),
    )
)

joined_adjuster = (
    prepared.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.status",
        "s.status_or_unknown",
        "a.adjuster_name",
    )
)

status_stats = (
    joined_adjuster
    .groupBy("status")
    .agg(F.avg("claimed_amount").alias("avg_claimed_amount"))
)

compared = (
    joined_adjuster.alias("s")
    .join(status_stats.alias("g"), F.col("g.status") == F.col("s.status"), "inner")
    .filter(F.col("s.claimed_amount") > F.col("g.avg_claimed_amount"))
    .select(
        "s.*",
        "g.avg_claimed_amount",
        F.round(F.col("s.claimed_amount") - F.col("g.avg_claimed_amount"), 2).alias("claimed_amount_minus_avg"),
    )
)

unique_rows = (
    compared
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    unique_rows
    .select("policy_id", "claim_date", "claimed_amount_minus_avg")
    .distinct()
    .orderBy(F.col("claim_date").desc(), "policy_id")
)
