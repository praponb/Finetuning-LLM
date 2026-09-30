from pyspark.sql import functions as F

in_window = (
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
        "c.adjuster_id",
        "c.status",
        "p.product",
        "p.premium",
    )
)

filtered = (
    in_window.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.approved_amount") >= 2349.3), "left_semi")
    .filter(F.col("claimed_amount") > 14019.8)
)

combined_details_adjuster = (
    filtered.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (~F.col("a.team").isin("Travel", "Property")), "inner")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .filter(F.col("a.team").isNotNull())
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.adjuster_id",
        "s.status",
        "s.product",
        "s.premium",
        F.col("a.team").alias("adjuster_team"),
        "a.adjuster_name",
        F.col("p.product").alias("policy_product"),
    )
)

result = (
    combined_details_adjuster
    .filter(F.col("premium") < 3521.8)
    .select(
        "id",
        "policy_id",
        "claim_date",
        "adjuster_id",
        "status",
        "product",
        "premium",
        "adjuster_team",
        "adjuster_name",
        F.concat_ws("|", F.col("adjuster_name"), F.col("product")).alias("adjuster_name_product_label"),
    )
    .orderBy("adjuster_team", "status", "id")
    .limit(5)
)
