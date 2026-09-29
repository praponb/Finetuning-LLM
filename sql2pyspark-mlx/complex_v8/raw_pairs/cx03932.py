from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_by_approved_amou = Window.partitionBy("adjuster_id").orderBy(F.col("approved_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_id_by_approved_amou_2 = Window.partitionBy("adjuster_id").orderBy(F.col("approved_amount").desc(), "id")
w_policy_id_status_by_approved = Window.partitionBy("policy_id", "status").orderBy("approved_amount", "id")
w_policy_product_policy_holder = Window.partitionBy("policy_product", "policy_holder_id")
w_policy_id_policy_holder_id_b = Window.partitionBy("policy_id", "policy_holder_id").orderBy(F.col("claimed_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_type_by_reported_at = Window.partitionBy("claim_type").orderBy("reported_at", "id")
w_distinct_ids_by_total_days_s = Window.partitionBy("distinct_ids").orderBy(F.col("total_days_since_prev_claim_date").desc(), "status").rowsBetween(-2, -1)
w_distinct_ids_by_total_days_s_2 = Window.partitionBy("distinct_ids").orderBy(F.col("total_days_since_prev_claim_date").desc(), "status").rowsBetween(-2, Window.currentRow)
w_status_distinct_ids = Window.partitionBy("status", "distinct_ids")

prepared = (
    df_claims
    .filter((F.col("adjuster_id") <= 7) & (F.month(F.col("reported_at")).isin(1, 2)))
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

with_details_policy = (
    prepared.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        F.col("p.premium").alias("policy_premium"),
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.product").alias("policy_product"),
    )
)

metrics = (
    with_details_policy
    .select(
        "*",
        F.nth_value("approved_amount", 2).over(w_adjuster_id_by_approved_amou).alias("second_approved_amount"),
        F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_adjuster_id_by_approved_amou_2)).alias("days_since_prev_claim_date"),
        F.lag("reported_at", 2).over(w_policy_id_status_by_approved).alias("prev2_reported_at"),
        F.ntile(3).over(w_adjuster_id_by_approved_amou_2).alias("approved_amount_tercile"),
        F.sum("policy_premium").over(w_policy_product_policy_holder).alias("policy_product_policy_holder_id_sum_policy_premi"),
        F.round(F.col("policy_premium") / F.nullif(F.sum("policy_premium").over(w_policy_product_policy_holder), F.lit(0)), 4).alias("policy_premium_share_of_policy_product_policy_ho"),
        F.last("claim_date", ignorenulls=True).over(w_policy_id_policy_holder_id_b).alias("last_known_claim_date"),
        F.dense_rank().over(w_claim_type_by_reported_at).alias("reported_at_dense_rank"),
        F.lead("approved_amount").over(w_adjuster_id_by_approved_amou_2).alias("next_approved_amount"),
        F.first("policy_premium").over(w_adjuster_id_by_approved_amou).alias("first_policy_premium"),
    )
)

derived = (
    metrics
    .withColumn("last_known_claim_date_month_start", F.trunc(F.col("last_known_claim_date"), "MM"))
)

grouped = (
    derived
    .groupBy("status")
    .agg(
        F.percentile_approx("next_approved_amount", 0.5).alias("median_next_approved_amount"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum("days_since_prev_claim_date").alias("total_days_since_prev_claim_date"),
        F.sum("policy_premium_share_of_policy_product_policy_ho").alias("total_policy_premium_share_of_policy_product_pol"),
    )
)

windowed = (
    grouped
    .select(
        "*",
        F.sum("median_next_approved_amount").over(w_distinct_ids_by_total_days_s).alias("median_next_approved_amount_sum_2_prior"),
        F.sum("distinct_ids").over(w_distinct_ids_by_total_days_s_2).alias("distinct_ids_sum_3"),
        F.sum("median_next_approved_amount").over(w_status_distinct_ids).alias("status_distinct_ids_sum_median_next_approved_amo"),
    )
)

result = (
    windowed
    .select("status", "distinct_ids")
    .distinct()
    .orderBy(F.col("status").desc(), F.col("distinct_ids").desc())
)
