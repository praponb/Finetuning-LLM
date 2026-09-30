from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("claim_type_group", "claim_type_group_2").orderBy("claim_date_month", "claim_type", "claim_date_month")

cleaned = (
    df_claims
    .filter((F.col("adjuster_id") <= 2) & (F.col("approved_amount") >= 4554.8))
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
        F.greatest(F.col("approved_amount"), F.col("claimed_amount")).alias("greatest_approved_amount_claimed_amount"),
    )
)

stats = (
    cleaned
    .groupBy(
        "claim_type",
        F.date_trunc("month", "claim_date").alias("claim_date_month"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("claim_type").alias("distinct_claim_types"),
        F.max("claim_date").alias("last_claim_date"),
        F.count(F.when(F.col("claim_type") == "fire", 1)).alias("n_claim_type_fire"),
        F.count(F.when(F.col("claim_type").isin("flood", "theft", "delay"), 1)).alias("n_claim_type_in_flood_theft_de"),
        F.sum("greatest_approved_amount_claimed_amount").alias("sum_greatest_approved_amount_claimed_amount_chec"),
    )
    .filter(F.col("sum_greatest_approved_amount_claimed_amount_chec") > 17500.0)
    .drop("sum_greatest_approved_amount_claimed_amount_chec")
)

annotated = (
    stats
    .filter(F.col("distinct_claim_types") >= 2)
    .select(
        "claim_type",
        "claim_date_month",
        "distinct_ids",
        "distinct_claim_types",
        "last_claim_date",
        "n_claim_type_in_flood_theft_de",
        F.when(F.col("claim_type").isin("collision", "flood"), "group_a").when(F.col("claim_type") == "fire", "group_b").otherwise("other").alias("claim_type_group"),
        F.when(F.col("claim_type").isin("collision", "medical"), "group_a").when(F.col("claim_type") == "theft", "group_b").otherwise("other").alias("claim_type_group_2"),
    )
)

with_activity = (
    annotated
    .crossJoin(annotated.agg(F.min("n_claim_type_in_flood_theft_de").alias("overall_min_n_claim_type_in_flood_theft_de")))
    .filter(F.col("n_claim_type_in_flood_theft_de") > F.col("overall_min_n_claim_type_in_flood_theft_de"))
    .drop("overall_min_n_claim_type_in_flood_theft_de")
)

unique_rows = (
    with_activity
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

tagged = (
    unique_rows
    .select(
        "claim_type",
        "claim_date_month",
        "distinct_claim_types",
        "n_claim_type_in_flood_theft_de",
        "claim_type_group",
        "claim_type_group_2",
        F.length(F.trim(F.col("claim_type_group_2"))).alias("claim_type_group_2_len"),
    )
)

result = (
    tagged
    .filter(F.col("claim_type_group_2_len") > 6)
    .select(
        "claim_type",
        "claim_date_month",
        "n_claim_type_in_flood_theft_de",
        "claim_type_group",
        "claim_type_group_2",
        "claim_type_group_2_len",
        F.round(F.col("claim_type_group_2_len") / F.nullif(F.col("distinct_claim_types"), F.lit(0)), 3).alias("claim_type_group_2_len_per_distinct_claim_types"),
        F.abs(F.col("distinct_claim_types") - 7).alias("distinct_claim_types_distance"),
    )
    .orderBy(F.col("claim_type").asc_nulls_last(), "claim_date_month")
    .limit(5)
)
