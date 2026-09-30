from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_date_month = Window.partitionBy(F.month("claim_date"))
w_all_by_approved_amount_full = Window.orderBy(F.col("approved_amount").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_approved_amount = Window.orderBy(F.col("approved_amount").desc_nulls_first(), "id")

prepared = (
    df_claims
    .filter((F.col("claimed_amount") < 44921.3) & (F.col("adjuster_id").isNotNull()))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.initcap(F.trim(F.col("status"))).alias("status_clean"),
        F.dayofweek(F.col("claim_date")).alias("claim_date_dayofweek"),
    )
)

labeled = (
    prepared
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "reported_at",
        "status_clean",
        F.substring(F.trim(F.col("claim_type")), 1, 4).alias("claim_type_prefix"),
        F.when(F.col("claimed_amount") >= 40638.4, "A").when(F.col("claimed_amount") >= 13635.1, "B").otherwise("C").alias("claimed_amount_tier"),
        F.coalesce(F.col("claim_type"), F.lit("UNKNOWN")).alias("claim_type_or_unknown"),
    )
)

metrics = (
    labeled
    .select(
        "*",
        F.size(F.collect_set("policy_id").over(w_claim_date_month)).alias("distinct_policy_id_in_claim_date_month"),
        F.max("approved_amount").over(w_claim_date_month).alias("claim_date_month_max_approved_amount"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_claim_date_month), 2).alias("approved_amount_vs_claim_date_month_avg"),
        F.first("claim_date").over(w_all_by_approved_amount_full).alias("first_claim_date"),
        F.size(F.collect_set("adjuster_id").over(w_claim_date_month)).alias("distinct_adjuster_id_in_claim_date_month"),
        F.row_number().over(w_all_by_approved_amount).alias("seq_no"),
    )
)

result = (
    metrics
    .filter(F.col("distinct_policy_id_in_claim_date_month") >= 4)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type_prefix",
        "claim_type_or_unknown",
        "approved_amount_vs_claim_date_month_avg",
        "first_claim_date",
        "distinct_adjuster_id_in_claim_date_month",
        F.datediff(F.col("claim_date"), F.col("first_claim_date")).alias("days_first_claim_date_to_claim_date"),
    )
    .orderBy(F.col("adjuster_id").asc_nulls_last())
)
