from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_claim_type = Window.partitionBy("product", "claim_type")

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
        "claim_date",
        "claimed_amount",
        "product",
        "premium",
        F.size(F.collect_set("policy_id").over(w_product_claim_type)).alias("distinct_policy_id_in_product_claim_type"),
        F.sum("premium").over(w_product_claim_type).alias("product_claim_type_sum_premium"),
        F.round(F.avg("approved_amount").over(w_product_claim_type), 2).alias("product_claim_type_avg_approved_amount"),
    )
)

product_stats = (
    ranked
    .groupBy("product")
    .agg(F.avg("product_claim_type_avg_approved_amount").alias("avg_product_claim_type_avg_approved_amount"))
)

compared = (
    ranked.alias("s")
    .join(product_stats.alias("g"), F.col("g.product") == F.col("s.product"), "inner")
    .filter(F.col("s.product_claim_type_avg_approved_amount") > F.col("g.avg_product_claim_type_avg_approved_amount"))
    .select(
        "s.*",
        "g.avg_product_claim_type_avg_approved_amount",
        F.round(F.col("s.product_claim_type_avg_approved_amount") - F.col("g.avg_product_claim_type_avg_approved_amount"), 2).alias("product_claim_type_avg_approved_amount_minus_avg"),
    )
)

result = (
    compared
    .filter(F.col("product_claim_type_sum_premium") <= 27590.0)
    .select(
        "id",
        "product",
        "distinct_policy_id_in_product_claim_type",
        "product_claim_type_sum_premium",
        "product_claim_type_avg_approved_amount",
        "avg_product_claim_type_avg_approved_amount",
        "product_claim_type_avg_approved_amount_minus_avg",
        F.date_format(F.col("claim_date"), "yyyy-MM").alias("claim_date_label"),
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
    )
    .orderBy("distinct_policy_id_in_product_claim_type")
)
