from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("status", "claim_status").orderBy(F.col("claimed_amount").desc(), F.col("id").desc(), F.col("claim_id").desc())
w_policy_id_reported_at_month = Window.partitionBy("policy_id", F.month("reported_at")).orderBy("claim_approved_amount", "id", "claim_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_reported_at_month = Window.partitionBy(F.date_trunc("month", "reported_at"))

prepared = (
    df_claims
    .filter(F.col("policy_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "status",
        "reported_at",
    )
)

exploded_detail = (
    prepared.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.claim_type").isNotNull()), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.status",
        "s.reported_at",
        F.col("c.id").alias("claim_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.status").alias("claim_status"),
        "c.claim_type",
    )
)

unique_rows = (
    exploded_detail
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

windowed = (
    unique_rows
    .select(
        "id",
        "policy_id",
        "claimed_amount",
        "status",
        "claim_id",
        "claim_approved_amount",
        "claim_type",
        F.nth_value("claim_approved_amount", 2).over(w_policy_id_reported_at_month).alias("second_claim_approved_amount"),
        F.size(F.collect_set("claim_id").over(w_reported_at_month)).alias("distinct_claim_id_in_reported_at_month"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_reported_at_month), 2).alias("claimed_amount_vs_reported_at_month_avg"),
        F.count(F.lit(1)).over(w_reported_at_month).alias("rows_in_reported_at_month"),
        F.size(F.collect_set("adjuster_id").over(w_reported_at_month)).alias("distinct_adjuster_id_in_reported_at_month"),
    )
)

rollup_base = (
    windowed
    .groupBy("distinct_claim_id_in_reported_at_month", "claim_type")
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.stddev("claimed_amount_vs_reported_at_month_avg"), 2).alias("stddev_claimed_amount_vs_reported_at_month_avg"),
        F.sum(F.when(F.col("claim_type").isin("medical", "flood", "delay"), F.col("claimed_amount"))).alias("claimed_amount_claim_type_in_medical_flood"),
        F.array_sort(F.collect_set("claim_type")).alias("claim_type_list"),
        F.percentile_approx("rows_in_reported_at_month", 0.5).alias("median_rows_in_reported_at_month"),
    )
)

with_activity = (
    rollup_base
    .crossJoin(rollup_base.agg(F.min("claimed_amount_claim_type_in_medical_flood").alias("overall_min_claimed_amount_claim_type_in_medical")))
    .filter(F.col("claimed_amount_claim_type_in_medical_flood") > F.col("overall_min_claimed_amount_claim_type_in_medical"))
    .drop("overall_min_claimed_amount_claim_type_in_medical")
)

scored = (
    with_activity
    .filter(F.col("cnt") < 10)
    .select(
        "*",
        F.when(F.col("cnt") >= 9, "HIGH").when(F.col("cnt") >= 4, "MEDIUM").otherwise("LOW").alias("cnt_tier"),
        (F.floor(F.col("cnt") / 2) * 2).alias("cnt_bucket"),
        F.when(F.col("cnt") >= 10, "large").when(F.col("cnt") >= 3, "medium").otherwise("small").alias("cnt_tier_2"),
    )
)

result = (
    scored
    .filter(F.col("cnt_tier") == "MEDIUM")
    .select(
        "distinct_claim_id_in_reported_at_month",
        "claim_type",
        "cnt",
        "stddev_claimed_amount_vs_reported_at_month_avg",
        "claimed_amount_claim_type_in_medical_flood",
        "median_rows_in_reported_at_month",
        "cnt_tier",
        "cnt_bucket",
        "cnt_tier_2",
        F.coalesce(F.col("cnt_tier_2"), F.lit("UNKNOWN")).alias("cnt_tier_2_or_unknown"),
    )
    .orderBy(
        F.col("stddev_claimed_amount_vs_reported_at_month_avg").desc(),
        F.col("cnt_tier").desc(),
    )
)
