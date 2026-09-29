from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_policy_id_by_cla = Window.partitionBy("adjuster_id", "policy_id").orderBy("claim_date", "id").rowsBetween(-3, Window.currentRow)
w_status_claim_date_month = Window.partitionBy("status", F.date_trunc("month", "claim_date"))
w_adjuster_id_policy_id_by_cla_2 = Window.partitionBy("adjuster_id", "policy_id").orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned_claims = (
    df_claims
    .filter(F.col("adjuster_id") >= 2)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
        F.coalesce(F.col("approved_amount"), F.lit(0.0)).alias("approved_amount_filled"),
    )
)

metrics = (
    cleaned_claims
    .select(
        "*",
        F.max("approved_amount").over(w_adjuster_id_policy_id_by_cla).alias("approved_amount_max_4"),
        F.size(F.collect_set("policy_id").over(w_status_claim_date_month)).alias("distinct_policy_id_in_status_claim_date_month"),
        F.first("approved_amount_filled").over(w_adjuster_id_policy_id_by_cla_2).alias("first_approved_amount_filled"),
    )
)

scored = (
    metrics
    .filter(F.col("approved_amount_max_4").between(30000.0, 97500.0))
    .select(
        "*",
        F.substring(F.trim(F.col("claim_type")), 1, 4).alias("claim_type_prefix"),
        F.greatest(F.col("approved_amount_filled"), F.col("approved_amount_max_4")).alias("greatest_approved_amount_filled_approved_amount_"),
        F.greatest(F.col("claimed_amount"), F.col("approved_amount")).alias("greatest_claimed_amount_approved_amount"),
    )
)

result = (
    scored
    .groupingSets(
        [["adjuster_id", "distinct_policy_id_in_status_claim_date_month", "status"], ["adjuster_id"], []],
        "adjuster_id", "distinct_policy_id_in_status_claim_date_month", "status",
    )
    .agg(
        F.grouping("adjuster_id").alias("is_total_adjuster_id"),
        F.count("*").alias("n_rows"),
        F.countDistinct("policy_id").alias("distinct_policy_ids"),
        F.countDistinct("claim_type").alias("distinct_claim_types"),
    )
    .orderBy(
        F.col("adjuster_id").asc_nulls_last(),
        F.col("distinct_policy_id_in_status_claim_date_month").asc_nulls_last(),
        F.col("status").asc_nulls_last(),
    )
)
