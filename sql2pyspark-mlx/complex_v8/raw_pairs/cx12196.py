from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_claim_type = Window.partitionBy("product", "claim_type")
w_policy_id_product_by_approve = Window.partitionBy("policy_id", "product").orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

matched = (
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
        "c.approved_amount",
        "c.claim_type",
        "p.product",
        "p.premium",
    )
)

metrics = (
    matched
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "product",
        F.sum("premium").over(w_product_claim_type).alias("product_claim_type_sum_premium"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_product_claim_type), F.lit(0)), 4).alias("premium_share_of_product_claim_type"),
        F.first("claimed_amount").over(w_policy_id_product_by_approve).alias("first_claimed_amount"),
        F.sum("claimed_amount").over(w_product_claim_type).alias("product_claim_type_sum_claimed_amount"),
    )
)

screened = (
    metrics.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.claimed_amount").between(10673.3, 38909.4)) & (F.col("c.reported_at") < F.lit("2025-03-08 14:00:00").cast("timestamp")), "left_semi")
)

with_details_policy = (
    screened.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.product",
        "s.product_claim_type_sum_premium",
        "s.product_claim_type_sum_claimed_amount",
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.product").alias("policy_product"),
    )
)

result = (
    with_details_policy
    .select(
        "id",
        "claim_date",
        "claimed_amount",
        "product",
        "product_claim_type_sum_premium",
        "product_claim_type_sum_claimed_amount",
        "policy_product",
        F.coalesce(F.col("claimed_amount"), F.lit(0.0)).alias("claimed_amount_filled"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("claim_date")).alias("days_since_claim_date"),
    )
    .orderBy("product_claim_type_sum_claimed_amount", F.col("id").desc())
)
