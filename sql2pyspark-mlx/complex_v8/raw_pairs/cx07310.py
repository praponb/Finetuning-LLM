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
        "c.status",
        "c.approved_amount",
        "p.product",
        "p.premium",
    )
)

combined_details = (
    in_window.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "inner")
    .select(
        "s.*",
        F.col("p.product").alias("policy_product"),
        F.col("po.region").alias("holder_region"),
        "po.holder_name",
    )
)

result = (
    combined_details
    .select(
        "id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "product",
        "premium",
        "policy_product",
        "holder_region",
        "holder_name",
        F.date_format(F.col("claim_date"), "yyyy-MM-dd").alias("claim_date_label"),
    )
    .orderBy(F.col("holder_region").asc_nulls_last())
)
