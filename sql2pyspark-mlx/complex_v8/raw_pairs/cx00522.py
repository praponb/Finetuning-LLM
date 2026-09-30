from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_date_month = Window.partitionBy(F.month("claim_date"))
w_claim_claim_type_count_by_cl = Window.partitionBy("claim_claim_type_count").orderBy("claimed_amount_adj", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_id_by_claimed_amount = Window.partitionBy("policy_id").orderBy(F.col("claimed_amount_adj").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_claims
    .filter(F.col("adjuster_id") >= 5)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "status",
        F.round(F.col("claimed_amount") * 1.1, 2).alias("claimed_amount_adj"),
        F.to_date(F.col("reported_at")).alias("reported_at_date"),
    )
)

enriched_adjuster = (
    filtered.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.claimed_amount_adj",
        "s.reported_at_date",
        "a.adjuster_name",
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.product").alias("policy_product"),
    )
)

screened = (
    enriched_adjuster.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.claim_date") >= F.lit("2024-06-27").cast("date")) & (F.col("c.approved_amount") < 19337.6), "left_semi")
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count(F.when(F.col("claim_type").isin("theft", "flood"), 1)).alias("claim_claim_type_count"),
        F.count(F.when(F.col("claim_type").isNotNull(), 1)).alias("claim_claim_type_count_2"),
        F.round(F.avg("approved_amount"), 2).alias("avg_claim_approved_amount"),
    )
)

summarized_rows = (
    screened.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
        F.coalesce(F.col("c.claim_claim_type_count_2"), F.lit(0)).alias("claim_claim_type_count_2"),
        "c.avg_claim_approved_amount",
    )
)

with_trends = (
    summarized_rows
    .select(
        "*",
        F.sum("claim_claim_type_count_2").over(w_claim_date_month).alias("claim_date_month_sum_claim_claim_type_count_2"),
        F.last("policy_end_date").over(w_claim_claim_type_count_by_cl).alias("last_policy_end_date"),
        F.count(F.lit(1)).over(w_claim_date_month).alias("rows_in_claim_date_month"),
        F.first("avg_claim_approved_amount").over(w_policy_id_by_claimed_amount).alias("first_avg_claim_approved_amount"),
    )
)

result = (
    with_trends
    .groupBy("last_policy_end_date")
    .pivot("policy_product", ["TRAVEL", "HOME"])
    .agg(F.count("avg_claim_approved_amount"))
    .orderBy("last_policy_end_date")
)
