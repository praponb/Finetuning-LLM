from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_max_approved_amount = Window.orderBy(F.col("max_approved_amount").desc(), "claim_date_month", "policy_id")
w_all_by_max_approved_amount_f = Window.orderBy(F.col("max_approved_amount").desc(), "claim_date_month", "policy_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_distinct_ids_full = Window.orderBy(F.col("distinct_ids").desc(), "claim_date_month", "policy_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared = (
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
        F.when(F.col("status").isin("PAID", "IN_REVIEW"), "group_a").when(F.col("status") == "OPEN", "group_b").otherwise("other").alias("status_group"),
        F.initcap(F.trim(F.col("claim_type"))).alias("claim_type_clean"),
        F.concat_ws("-", F.col("status"), F.col("claim_type")).alias("status_claim_type_label"),
    )
)

summary = (
    prepared
    .filter(F.col("approved_amount") <= 32511.8)
    .groupBy(
        F.date_trunc("month", "claim_date").alias("claim_date_month"),
        "policy_id",
    )
    .agg(
        F.min("claim_date").alias("first_claim_date"),
        F.max("approved_amount").alias("max_approved_amount"),
        F.datediff(F.max("claim_date"), F.min("claim_date")).alias("claim_date_span_days"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("claim_type") == "medical", 1)).alias("n_claim_type_medical"),
        F.max("claimed_amount").alias("max_claimed_amount_check"),
    )
    .filter(F.col("max_claimed_amount_check") > 4234.9)
    .drop("max_claimed_amount_check")
)

with_parent = (
    summary.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.claim_date_month",
        "s.policy_id",
        "s.max_approved_amount",
        "s.claim_date_span_days",
        "s.distinct_ids",
        F.col("p.status").alias("policy_status"),
    )
)

filtered = (
    with_parent.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.claim_type") != "medical") & (F.col("c.claimed_amount") > 13898.1), "left_semi")
    .filter(F.col("claim_date_month") >= F.lit("2025-01-25 09:00:00").cast("timestamp"))
)

windowed = (
    filtered
    .select(
        "*",
        F.lag("max_approved_amount", 3).over(w_all_by_max_approved_amount).alias("prev3_max_approved_amount"),
        (F.col("claim_date_span_days") - F.lag("claim_date_span_days").over(w_all_by_max_approved_amount)).alias("claim_date_span_days_change"),
        F.last("claim_date_month").over(w_all_by_max_approved_amount_f).alias("last_claim_date_month"),
        F.first("max_approved_amount").over(w_all_by_distinct_ids_full).alias("first_max_approved_amount"),
    )
)

result = (
    windowed
    .select(
        "claim_date_month",
        "policy_id",
        "max_approved_amount",
        "claim_date_span_days",
        "distinct_ids",
        "policy_status",
        "prev3_max_approved_amount",
        "last_claim_date_month",
        "first_max_approved_amount",
        F.greatest(F.col("distinct_ids"), F.col("max_approved_amount")).alias("greatest_distinct_ids_max_approved_amount"),
    )
    .orderBy("distinct_ids")
)
