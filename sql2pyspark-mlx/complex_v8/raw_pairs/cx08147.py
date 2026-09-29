from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_status = Window.partitionBy("claim_status")
w_all_by_claimed_amount = Window.orderBy(F.col("claimed_amount").desc(), "id", "claim_id")

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
        "c.status",
        "c.adjuster_id",
        "p.product",
        "p.premium",
    )
)

enriched = (
    joined_ranges
    .filter(F.col("claim_date") >= F.lit("2024-06-13").cast("date"))
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "status",
        "product",
        "premium",
        F.when(F.col("product").isin("TRAVEL", "HEALTH"), "group_a").when(F.col("product") == "HOME", "group_b").otherwise("other").alias("product_group"),
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
    )
)

with_children = (
    enriched.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.product_group",
        "s.premium_filled",
        F.col("c.id").alias("claim_id"),
        F.col("c.status").alias("claim_status"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        "c.claim_type",
    )
)

with_windows = (
    with_children
    .select(
        "id",
        "policy_id",
        "claim_date",
        "product_group",
        "premium_filled",
        "claim_id",
        "claim_approved_amount",
        "claim_type",
        F.round(F.col("premium_filled") - F.avg("premium_filled").over(w_claim_status), 2).alias("premium_filled_vs_claim_status_avg"),
        F.round(F.cume_dist().over(w_all_by_claimed_amount), 3).alias("claimed_amount_cume_dist"),
        F.round(F.col("claim_approved_amount") - F.avg("claim_approved_amount").over(w_claim_status), 2).alias("claim_approved_amount_vs_claim_status_avg"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_claim_status), F.lit(0)), 4).alias("claimed_amount_share_of_claim_status"),
        F.round(F.col("premium_filled") / F.nullif(F.sum("premium_filled").over(w_claim_status), F.lit(0)), 4).alias("premium_filled_share_of_claim_status"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_claim_status), 2).alias("claimed_amount_vs_claim_status_avg"),
    )
)

result = (
    with_windows
    .groupingSets(
        [["claim_id", "policy_id", "product_group"], ["claim_id"], []],
        "claim_id", "policy_id", "product_group",
    )
    .agg(F.count("*").alias("n_rows"))
    .orderBy(
        F.col("claim_id").asc_nulls_last(),
        F.col("policy_id").asc_nulls_last(),
        F.col("product_group").asc_nulls_last(),
    )
)
