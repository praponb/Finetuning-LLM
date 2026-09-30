from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("adjuster_id", "claim_type").orderBy("claimed_amount", "id")

base = (
    df_claims
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

unique_rows = (
    base
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

policy_id_stats = (
    unique_rows
    .groupBy("policy_id")
    .agg(F.percentile_approx("approved_amount", 0.5).alias("median_approved_amount"))
)

vs_group = (
    unique_rows.alias("s")
    .join(policy_id_stats.alias("g"), F.col("g.policy_id") == F.col("s.policy_id"), "inner")
    .filter(F.col("s.approved_amount") >= F.col("g.median_approved_amount"))
    .select(
        "s.*",
        "g.median_approved_amount",
        F.round(F.col("s.approved_amount") - F.col("g.median_approved_amount"), 2).alias("approved_amount_minus_median"),
    )
)

qualified = (
    vs_group.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.premium").between(1051.9, 4417.0)), "left_semi")
)

result = (
    qualified
    .rollup("status", "policy_id")
    .agg(
        F.grouping_id().alias("gid"),
        F.max("claimed_amount").alias("max_claimed_amount"),
        F.min("reported_at").alias("first_reported_at"),
    )
    .select(
        F.coalesce(F.col("status"), F.lit("ALL")).alias("status"),
        "policy_id",
        "gid",
        "max_claimed_amount",
        "first_reported_at",
    )
    .orderBy(F.col("status").asc_nulls_last(), F.col("policy_id").asc_nulls_last())
)
