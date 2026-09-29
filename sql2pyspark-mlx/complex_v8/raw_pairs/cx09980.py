from pyspark.sql import functions as F

in_window = (
    df_claims.alias("c")
    .join(
        df_policies.alias("p"),
        (F.col("p.id") == F.col("c.policy_id"))
        & F.col("c.claim_date").between(F.col("p.start_date"), F.coalesce(F.col("p.end_date"), F.lit("2099-12-31").cast("date"))),
        "left",
    )
    .select(
        "c.id",
        "c.policy_id",
        "c.claim_date",
        "c.claimed_amount",
        "c.adjuster_id",
        "c.claim_type",
        "p.product",
        "p.premium",
    )
)

annotated = (
    in_window
    .filter(F.col("adjuster_id") <= 6)
    .select(
        "*",
        F.round(F.col("premium") * 0.25, 2).alias("premium_adj"),
        F.concat_ws("-", F.col("claim_type"), F.col("product")).alias("claim_type_product_label"),
        F.round(F.col("claimed_amount") * 1.1, 2).alias("claimed_amount_adj"),
    )
)

combined_details_adjuster = (
    annotated.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select("s.*", F.col("a.team").alias("adjuster_team"), "a.adjuster_name")
)

result = (
    combined_details_adjuster
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claim_type",
        "product",
        "premium_adj",
        "claim_type_product_label",
        "claimed_amount_adj",
        "adjuster_team",
        F.when(F.col("claimed_amount_adj") >= 60020.0, "A").when(F.col("claimed_amount_adj") >= 22570.0, "B").otherwise("C").alias("claimed_amount_adj_tier"),
        F.coalesce(F.col("product"), F.lit("UNKNOWN")).alias("product_or_unknown"),
    )
    .orderBy(F.col("premium_adj").desc(), "id")
    .limit(10)
)
