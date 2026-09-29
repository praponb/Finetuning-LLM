from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_reported_at_month_by_c = Window.partitionBy(F.date_trunc("month", "claim_reported_at")).orderBy("claim_claimed_amount", "id", "claim_id").rowsBetween(-3, -1)
w_claim_reported_at_month_by_c_2 = Window.partitionBy(F.date_trunc("month", "claim_reported_at")).orderBy("claim_claimed_amount", "id", "claim_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_count_adjuster_id_by_c = Window.partitionBy("claim_count", "adjuster_id").orderBy(F.col("claim_reported_at").asc_nulls_last(), "id", "claim_id").rowsBetween(-1, Window.currentRow)
w_policy_id_claim_id_by_approv = Window.partitionBy("policy_id", "claim_id").orderBy("approved_amount", "id", "claim_id").rowsBetween(-1, -1)

filtered = (
    df_claims
    .filter((F.col("claim_date").between(F.lit("2024-06-26").cast("date"), F.lit("2025-08-25").cast("date"))) & (F.to_date(F.col("reported_at")) < F.lit("2025-03-08").cast("date")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.substring(F.trim(F.col("claim_type")), 1, 2).alias("claim_type_prefix"),
        F.when(F.col("claim_type").isin("fire", "delay"), "group_a").when(F.col("claim_type") == "theft", "group_b").otherwise("other").alias("claim_type_group"),
    )
)

annotated = (
    filtered
    .withColumn("approved_amount_bucket", F.floor(F.col("approved_amount") / 10000) * 10000)
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.count("*").alias("claim_count"))
)

profiled = (
    annotated.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

exploded_detail = (
    profiled.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.claimed_amount") <= 42838.9), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.approved_amount",
        "s.reported_at",
        "s.claim_type_prefix",
        "s.claim_type_group",
        "s.approved_amount_bucket",
        "s.claim_count",
        F.col("c.id").alias("claim_id"),
        "c.claim_date",
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.reported_at").alias("claim_reported_at"),
    )
)

sequenced = (
    exploded_detail
    .select(
        "id",
        "policy_id",
        "claim_type_prefix",
        "claim_type_group",
        "approved_amount_bucket",
        "claim_count",
        "claim_id",
        "claim_date",
        "claim_claimed_amount",
        "claim_reported_at",
        F.round(F.avg("approved_amount").over(w_claim_reported_at_month_by_c), 2).alias("approved_amount_avg_3_prior"),
        F.max("approved_amount_bucket").over(w_claim_reported_at_month_by_c_2).alias("max_approved_amount_bucket_so_far"),
        F.round(F.avg("claim_claimed_amount").over(w_claim_count_adjuster_id_by_c), 2).alias("claim_claimed_amount_avg_2"),
        F.max("approved_amount_bucket").over(w_policy_id_claim_id_by_approv).alias("approved_amount_bucket_max_1_prior"),
    )
)

exploded_detail_claims = (
    sequenced.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_type_prefix",
        "s.claim_type_group",
        "s.approved_amount_bucket",
        "s.claim_count",
        "s.claim_id",
        "s.claim_claimed_amount",
        "s.claim_reported_at",
        "s.max_approved_amount_bucket_so_far",
        "s.claim_claimed_amount_avg_2",
        "s.approved_amount_bucket_max_1_prior",
        F.col("c.id").alias("claim_id_2"),
        "c.claim_type",
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.status").alias("claim_status"),
    )
)

filtered_2 = (
    exploded_detail_claims.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (~F.col("c.claim_type").isin("theft", "collision")), "left_anti")
)

result = (
    filtered_2
    .select(
        "id",
        "policy_id",
        "claim_type_prefix",
        "claim_type_group",
        "approved_amount_bucket",
        "claim_id",
        "claim_claimed_amount",
        "claim_reported_at",
        "max_approved_amount_bucket_so_far",
        "claim_claimed_amount_avg_2",
        "approved_amount_bucket_max_1_prior",
        "claim_id_2",
        "claim_type",
        "claim_approved_amount",
        (F.col("claim_approved_amount") + F.col("claim_count")).alias("claim_approved_amount_plus_claim_count"),
    )
    .orderBy(F.col("claim_approved_amount_plus_claim_count").desc())
)
