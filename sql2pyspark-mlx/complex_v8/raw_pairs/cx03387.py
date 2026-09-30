from pyspark.sql import functions as F

covered = (
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
        "c.approved_amount",
        "p.product",
        "p.premium",
    )
)

labeled = (
    covered
    .filter(F.col("claim_date").between(F.lit("2024-04-09").cast("date"), F.lit("2025-08-25").cast("date")))
    .select(
        "*",
        F.concat_ws(" / ", F.col("status"), F.col("product")).alias("status_product_label"),
        F.substring(F.trim(F.col("status")), 1, 2).alias("status_prefix"),
    )
)

result = (
    labeled
    .select(
        "id",
        "policy_id",
        "claim_date",
        "status",
        "approved_amount",
        "product",
        "premium",
        "status_product_label",
        "status_prefix",
        F.date_trunc("month", F.col("claim_date")).alias("claim_date_month"),
    )
    .orderBy(F.col("premium").desc_nulls_first(), F.col("policy_id").desc(), "id")
    .limit(5)
)
