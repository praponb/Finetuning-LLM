from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_by_claimed_amount = Window.partitionBy("policy_id").orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claim_type = Window.partitionBy("claim_type")

joined_ranges = (
    df_claims.alias("c")
    .join(
        df_policies.alias("p"),
        (F.col("p.id") == F.col("c.policy_id"))
        & F.col("c.claim_date").between(F.col("p.start_date"), F.coalesce(F.col("p.end_date"), F.lit("2099-12-31").cast("date"))),
        "inner",
    )
    .select(
        "c.id",
        "c.policy_id",
        "c.claim_date",
        "c.claimed_amount",
        "c.claim_type",
        "c.approved_amount",
        "p.product",
        "p.premium",
    )
)

ranked = (
    joined_ranges
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "premium",
        F.nth_value("approved_amount", 2).over(w_policy_id_by_claimed_amount).alias("second_approved_amount"),
        F.round(F.avg("premium").over(w_claim_type), 2).alias("claim_type_avg_premium"),
        F.size(F.collect_set("product").over(w_claim_type)).alias("distinct_product_in_claim_type"),
        F.round(F.avg("claimed_amount").over(w_claim_type), 2).alias("claim_type_avg_claimed_amount"),
    )
)

with_details = (
    ranked.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.status") != "CANCELLED"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.premium",
        "s.second_approved_amount",
        "s.distinct_product_in_claim_type",
        "s.claim_type_avg_claimed_amount",
        F.col("p.premium").alias("policy_premium"),
        F.col("p.holder_id").alias("policy_holder_id"),
    )
)

result = (
    with_details
    .select(
        "id",
        "policy_id",
        "premium",
        "second_approved_amount",
        "distinct_product_in_claim_type",
        "claim_type_avg_claimed_amount",
        "policy_holder_id",
        F.coalesce(F.col("policy_premium"), F.lit(0.0)).alias("policy_premium_filled"),
    )
    .orderBy(F.col("premium").asc_nulls_last(), "distinct_product_in_claim_type")
)
