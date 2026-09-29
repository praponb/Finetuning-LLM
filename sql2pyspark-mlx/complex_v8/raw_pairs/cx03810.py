from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("adjuster_id", "policy_id").orderBy(F.col("claimed_amount").desc(), F.col("id").desc())

prepared = (
    df_claims
    .filter(F.col("claimed_amount") <= 32441.2)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        F.coalesce(F.col("claimed_amount"), F.lit(0.0)).alias("claimed_amount_filled"),
    )
)

enriched = (
    prepared
    .filter(F.col("claimed_amount").between(12726.7, 41878.5))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        "claimed_amount_filled",
        F.lower(F.trim(F.col("status"))).alias("status_clean"),
    )
)

filtered = (
    enriched.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.status") == "OPEN"), "left_semi")
)

filtered_2 = (
    filtered.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.status") == "LAPSED") & (F.col("p.product") == "HOME"), "left_semi")
)

joined_policy = (
    filtered_2.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.reported_at",
        "s.claimed_amount_filled",
        "s.status_clean",
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.start_date").alias("policy_start_date"),
        F.col("p.end_date").alias("policy_end_date"),
    )
)

unique_rows = (
    joined_policy
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

grouped = (
    unique_rows
    .groupBy("policy_holder_id", "adjuster_id")
    .agg(
        F.percentile_approx("claimed_amount_filled", 0.5).alias("median_claimed_amount_filled"),
        F.max("claimed_amount").alias("max_claimed_amount_check"),
    )
    .filter(F.col("max_claimed_amount_check") > 10518.4)
    .drop("max_claimed_amount_check")
)

result = (
    grouped
    .select("policy_holder_id")
    .orderBy(F.col("policy_holder_id").desc_nulls_first())
)
