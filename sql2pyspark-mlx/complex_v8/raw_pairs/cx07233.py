from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_status = Window.partitionBy("product", "status")
w_status_by_premium_moving_1 = Window.partitionBy("status").orderBy(F.col("premium").desc(), "id").rowsBetween(-1, Window.currentRow)
w_status_by_premium = Window.partitionBy("status").orderBy(F.col("premium").desc(), "id")
w_status_by_premium_full = Window.partitionBy("status").orderBy(F.col("premium").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared = (
    df_policies
    .filter(F.col("holder_id").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

with_trends = (
    prepared
    .select(
        "*",
        F.count(F.lit(1)).over(w_product_status).alias("rows_in_product_status"),
        F.max("premium").over(w_status_by_premium_moving_1).alias("premium_max_2"),
        F.dense_rank().over(w_status_by_premium).alias("premium_dense_rank"),
        F.last("end_date").over(w_status_by_premium_full).alias("last_end_date"),
        F.size(F.collect_set("holder_id").over(w_product_status)).alias("distinct_holder_id_in_product_status"),
    )
)

annotated = (
    with_trends
    .filter(F.col("premium_dense_rank") < 6)
    .select(
        "id",
        "holder_id",
        "end_date",
        "rows_in_product_status",
        "premium_max_2",
        "premium_dense_rank",
        "distinct_holder_id_in_product_status",
        F.datediff(F.col("start_date"), F.col("last_end_date")).alias("days_last_end_date_to_start_date"),
    )
)

with_children = (
    annotated.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claim_type") == "theft"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.end_date",
        "s.rows_in_product_status",
        "s.premium_max_2",
        "s.premium_dense_rank",
        "s.distinct_holder_id_in_product_status",
        F.col("c.id").alias("claim_id"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        "c.claim_date",
        "c.claim_type",
    )
)

enriched_claim = (
    with_children.alias("s")
    .join(df_claims.alias("c"), F.col("c.id") == F.col("s.claim_id"), "inner")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("c.policy_id"), "left")
    .filter(F.col("c.approved_amount") > 4554.8)
    .select(
        "s.id",
        "s.holder_id",
        "s.end_date",
        "s.rows_in_product_status",
        "s.premium_max_2",
        "s.distinct_holder_id_in_product_status",
        "s.claim_id",
        "s.claim_claimed_amount",
        "s.claim_type",
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("p.product").alias("policy_product"),
        F.col("p.premium").alias("policy_premium"),
    )
)

scored = (
    enriched_claim
    .select(
        "id",
        "holder_id",
        "end_date",
        "premium_max_2",
        "distinct_holder_id_in_product_status",
        "claim_id",
        "claim_approved_amount",
        "policy_product",
        "policy_premium",
        F.round(F.col("claim_approved_amount") * 0.9, 2).alias("claim_approved_amount_adj"),
        F.when(F.col("policy_product").isin("HOME", "TRAVEL"), "group_a").when(F.col("policy_product") == "LIFE", "group_b").otherwise("other").alias("policy_product_group"),
        F.greatest(F.col("policy_premium"), F.col("claim_claimed_amount")).alias("greatest_policy_premium_claim_claimed_amount"),
    )
)

enriched = (
    scored.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .join(df_claims.alias("c"), F.col("c.id") == F.col("s.claim_id"), "inner")
    .filter(F.year(F.col("p.joined_on")) == 2024)
    .select(
        "s.id",
        "s.holder_id",
        "s.premium_max_2",
        "s.claim_id",
        "s.claim_approved_amount",
        "s.policy_product",
        "s.policy_premium",
        "s.claim_approved_amount_adj",
        "s.greatest_policy_premium_claim_claimed_amount",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("c.status").alias("claim_status"),
        "c.claim_type",
    )
)

result = (
    enriched
    .filter(F.col("holder_id").isNotNull())
    .select(
        "id",
        "holder_id",
        "premium_max_2",
        "claim_approved_amount",
        "policy_premium",
        "claim_approved_amount_adj",
        "greatest_policy_premium_claim_claimed_amount",
        "holder_segment",
        "holder_joined_on",
        "claim_status",
        F.greatest(F.col("policy_premium"), F.col("premium_max_2")).alias("greatest_policy_premium_premium_max_2"),
    )
    .orderBy(
        F.col("id").asc_nulls_last(),
        F.col("policy_premium").desc_nulls_first(),
    )
)
