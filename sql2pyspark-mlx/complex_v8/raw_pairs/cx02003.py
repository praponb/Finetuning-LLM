from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_count = Window.partitionBy("claim_count")
w_claim_date_dayofweek_by_clai = Window.partitionBy("claim_date_dayofweek").orderBy(F.col("claim_date").desc(), "id")
w_policy_product_by_claim_date = Window.partitionBy("policy_product").orderBy("claim_date_tercile", "id").rowsBetween(-2, -1)
w_policy_product_by_claim_date_2 = Window.partitionBy("policy_product").orderBy("claim_date_tercile", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = df_claims.filter(F.col("adjuster_id").isNotNull())

enriched = (
    prepared
    .filter(F.col("adjuster_id") >= 6)
    .select(
        "*",
        F.concat(F.col("claim_type"), F.lit("#"), F.col("policy_id").cast("string")).alias("claim_type_ref"),
        F.dayofweek(F.col("claim_date")).alias("claim_date_dayofweek"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(F.count("*").alias("claim_count"))
)

with_counts = (
    enriched.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

with_trends = (
    with_counts
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "status",
        "reported_at",
        "claim_type_ref",
        F.round(F.col("claim_count") - F.avg("claim_count").over(w_claim_count), 2).alias("claim_count_vs_claim_count_avg"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_claim_count), 2).alias("approved_amount_vs_claim_count_avg"),
        F.min("claim_count").over(w_claim_count).alias("claim_count_min_claim_count"),
        F.ntile(3).over(w_claim_date_dayofweek_by_clai).alias("claim_date_tercile"),
    )
)

joined = (
    with_trends.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .filter(F.col("p.end_date").between(F.lit("2025-06-08").cast("date"), F.lit("2026-09-19").cast("date")))
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        "s.claim_count_vs_claim_count_avg",
        "s.approved_amount_vs_claim_count_avg",
        "s.claim_count_min_claim_count",
        "s.claim_date_tercile",
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.status").alias("policy_status"),
        F.col("p.product").alias("policy_product"),
    )
)

with_windows = (
    joined
    .withColumn("approved_amount_avg_2_prior", F.round(F.avg("approved_amount").over(w_policy_product_by_claim_date), 2))
    .withColumn("first_known_claim_count_min_claim_count", F.first("claim_count_min_claim_count", ignorenulls=True).over(w_policy_product_by_claim_date_2))
)

result = (
    with_windows
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "status",
        "claim_count_vs_claim_count_avg",
        "approved_amount_vs_claim_count_avg",
        "claim_date_tercile",
        "policy_end_date",
        "policy_status",
        "policy_product",
        "approved_amount_avg_2_prior",
        "first_known_claim_count_min_claim_count",
        F.substring(F.trim(F.col("status")), 1, 4).alias("status_prefix"),
    )
    .orderBy("status_prefix", F.col("status").desc())
)
