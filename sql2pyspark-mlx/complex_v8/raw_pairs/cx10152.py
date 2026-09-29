from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_claim_type = Window.partitionBy("adjuster_id", "claim_type")
w_adjuster_id_reported_at_mont = Window.partitionBy("adjuster_id", F.month("reported_at")).orderBy(F.col("approved_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_id_reported_at_mont_2 = Window.partitionBy("adjuster_id", F.month("reported_at")).orderBy(F.col("approved_amount").desc(), "id").rowsBetween(-1, -1)
w_adjuster_id_reported_at_mont_3 = Window.partitionBy("adjuster_id", F.month("reported_at")).orderBy(F.col("approved_amount").desc(), "id")
w_adjuster_id_reported_at_mont_4 = Window.partitionBy("adjuster_id", F.month("reported_at")).orderBy(F.col("approved_amount").desc())

filtered = (
    df_claims
    .filter(F.col("policy_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.size(F.collect_set("policy_id").over(w_adjuster_id_claim_type)).alias("distinct_policy_id_in_adjuster_id_claim_type"),
        F.nth_value("reported_at", 2).over(w_adjuster_id_reported_at_mont).alias("second_reported_at"),
        F.round(F.avg("approved_amount").over(w_adjuster_id_reported_at_mont_2), 2).alias("approved_amount_avg_1_prior"),
        F.dense_rank().over(w_adjuster_id_reported_at_mont_3).alias("approved_amount_dense_rank"),
        F.lead("reported_at").over(w_adjuster_id_reported_at_mont_3).alias("next_reported_at"),
        F.round(F.percent_rank().over(w_adjuster_id_reported_at_mont_4), 3).alias("approved_amount_percent_rank"),
    )
)

adjuster_id_stats = (
    metrics
    .groupBy("adjuster_id")
    .agg(F.percentile_approx("approved_amount_avg_1_prior", 0.5).alias("median_approved_amount_avg_1_prior"))
)

compared = (
    metrics.alias("s")
    .join(adjuster_id_stats.alias("g"), F.col("g.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .filter(F.col("s.approved_amount_avg_1_prior") >= F.col("g.median_approved_amount_avg_1_prior"))
    .select(
        "s.*",
        "g.median_approved_amount_avg_1_prior",
        F.round(F.col("s.approved_amount_avg_1_prior") - F.col("g.median_approved_amount_avg_1_prior"), 2).alias("approved_amount_avg_1_prior_minus_median"),
    )
)

with_activity = (
    compared.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left_semi")
)

result = (
    with_activity
    .groupingSets(
        [["claim_type", "approved_amount_dense_rank", "distinct_policy_id_in_adjuster_id_claim_type"], ["claim_type"], ["approved_amount_dense_rank"]],
        "claim_type", "approved_amount_dense_rank", "distinct_policy_id_in_adjuster_id_claim_type",
    )
    .agg(
        F.min("next_reported_at").alias("first_next_reported_at"),
        F.round(100 * F.count(F.when(F.col("claim_type").isNotNull(), 1)) / F.count("*"), 1).alias("pct_claim_type_is_not_null"),
        F.array_sort(F.collect_set("claim_type")).alias("claim_type_list"),
    )
    .orderBy(
        F.col("claim_type").asc_nulls_last(),
        F.col("approved_amount_dense_rank").asc_nulls_last(),
        F.col("distinct_policy_id_in_adjuster_id_claim_type").asc_nulls_last(),
    )
)
