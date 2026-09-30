from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_start_date_prior_3 = Window.orderBy("start_date", "id").rowsBetween(-3, -1)
w_holder_id_by_min_claim_appro = Window.partitionBy("holder_id").orderBy(F.col("min_claim_approved_amount").desc(), "id")

recent_policys = (
    df_policies
    .filter((F.col("start_date") >= F.lit("2024-07-19").cast("date")) | (F.col("product") == "TRAVEL"))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        F.coalesce(F.col("product"), F.lit("UNKNOWN")).alias("product_or_unknown"),
        F.when(F.col("product").isin("HEALTH", "LIFE"), "group_a").when(F.col("product") == "TRAVEL", "group_b").otherwise("other").alias("product_group"),
        F.concat(F.col("product"), F.lit("#"), F.col("holder_id").cast("string")).alias("product_ref"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.min("approved_amount").alias("min_claim_approved_amount"))
)

summarized_rows = (
    recent_policys.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select("s.*", "c.min_claim_approved_amount")
)

derived = (
    summarized_rows
    .withColumn("product_group_group", F.when(F.col("product_group").isin("other", "group_a"), "group_a").when(F.col("product_group") == "group_b", "group_b").otherwise("other"))
)

windowed = (
    derived
    .select(
        "id",
        "start_date",
        "end_date",
        "product_or_unknown",
        "product_group",
        "product_ref",
        "product_group_group",
        F.sum("premium").over(w_all_by_start_date_prior_3).alias("premium_sum_3_prior"),
        F.row_number().over(w_holder_id_by_min_claim_appro).alias("seq_no"),
    )
)

claim_stats_2 = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.min("reported_at").alias("first_claim_reported_at"),
        F.count("*").alias("claim_count"),
    )
)

with_stats = (
    windowed.alias("s")
    .join(claim_stats_2.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "c.first_claim_reported_at",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

top_rows = (
    with_stats
    .filter((F.col("seq_no") <= 5) & (F.col("end_date") >= F.lit("2026-02-16").cast("date")))
    .drop("seq_no")
)

claim_stats_3 = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count("*").alias("claim_count_2"),
        F.count(F.when(~F.col("status").isin("IN_REVIEW", "REJECTED"), 1)).alias("claim_status_count"),
        F.min("reported_at").alias("first_claim_reported_at_2"),
    )
)

with_stats_2 = (
    top_rows.alias("s")
    .join(claim_stats_3.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count_2"), F.lit(0)).alias("claim_count_2"),
        F.coalesce(F.col("c.claim_status_count"), F.lit(0)).alias("claim_status_count"),
        "c.first_claim_reported_at_2",
    )
)

result = (
    with_stats_2
    .filter(F.col("first_claim_reported_at_2") < F.lit("2025-02-13 09:00:00").cast("timestamp"))
    .select(
        "id",
        "end_date",
        "product_or_unknown",
        "product_group",
        "product_group_group",
        "premium_sum_3_prior",
        "claim_count",
        "claim_count_2",
        "claim_status_count",
        "first_claim_reported_at_2",
    )
    .orderBy("claim_count_2")
)
