from pyspark.sql import functions as F

joined_ranges = (
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
        "c.status",
        "c.adjuster_id",
        "p.product",
        "p.premium",
    )
)

with_parent = (
    joined_ranges.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select("s.*", F.col("p.start_date").alias("policy_start_date"))
)

labeled = (
    with_parent
    .select(
        "*",
        F.year(F.col("policy_start_date")).alias("policy_start_date_year"),
        F.concat_ws(": ", F.col("product"), F.col("status")).alias("product_status_label"),
    )
)

result = (
    labeled
    .filter(F.col("claimed_amount").between(10518.4, 32441.2))
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "status",
        "adjuster_id",
        "product",
        "policy_start_date",
        "product_status_label",
        F.length(F.trim(F.col("product_status_label"))).alias("product_status_label_len"),
    )
    .orderBy(F.col("policy_id").desc(), "policy_start_date")
)
