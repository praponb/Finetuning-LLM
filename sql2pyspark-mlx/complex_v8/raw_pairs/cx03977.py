from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_max_approved_amount = Window.orderBy(F.col("max_approved_amount").desc(), "claim_type_or_unknown", "claim_date_month_month")
w_all_by_max_approved_amount_f = Window.orderBy(F.col("max_approved_amount").desc(), "claim_type_or_unknown", "claim_date_month_month").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_n_claim_type_medical = Window.partitionBy("n_claim_type_medical")

scoped_claims = (
    df_claims
    .filter((~F.col("status").isin("OPEN", "PAID")) | (F.col("policy_id").isNotNull()))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.coalesce(F.col("claim_type"), F.lit("UNKNOWN")).alias("claim_type_or_unknown"),
        F.date_trunc("month", F.col("claim_date")).alias("claim_date_month"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.min("claim_date").alias("first_claim_date"))
)

with_stats = (
    scoped_claims.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select("s.*", "c.first_claim_date")
)

per_group = (
    with_stats
    .filter(F.col("claim_type").isNotNull())
    .groupBy(
        "claim_type_or_unknown",
        F.date_trunc("month", "claim_date_month").alias("claim_date_month_month"),
    )
    .agg(
        F.max("approved_amount").alias("max_approved_amount"),
        F.count(F.when(F.col("claim_type") != "medical", 1)).alias("n_claim_type_medical"),
        F.min("approved_amount").alias("min_approved_amount"),
        F.array_sort(F.collect_set("claim_type")).alias("claim_type_list"),
        F.sum("claimed_amount").alias("sum_claimed_amount_check"),
    )
    .filter(F.col("sum_claimed_amount_check") > 4868.7)
    .drop("sum_claimed_amount_check")
)

with_windows = (
    per_group
    .select(
        "*",
        (F.col("n_claim_type_medical") - F.lag("n_claim_type_medical").over(w_all_by_max_approved_amount)).alias("n_claim_type_medical_change"),
        F.nth_value("max_approved_amount", 2).over(w_all_by_max_approved_amount_f).alias("second_max_approved_amount"),
        F.size(F.collect_set("claim_type_or_unknown").over(w_n_claim_type_medical)).alias("distinct_claim_type_or_unknown_in_n_claim_type_m"),
    )
)

result = (
    with_windows
    .select(
        "claim_type_or_unknown",
        "claim_date_month_month",
        "n_claim_type_medical_change",
        "distinct_claim_type_or_unknown_in_n_claim_type_m",
    )
    .unpivot(
        ids=["claim_type_or_unknown", "claim_date_month_month"],
        values=["n_claim_type_medical_change", "distinct_claim_type_or_unknown_in_n_claim_type_m"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
