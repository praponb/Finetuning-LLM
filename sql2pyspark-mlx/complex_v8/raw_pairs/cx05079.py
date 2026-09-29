from pyspark.sql import functions as F

valid = (
    df_policies
    .filter(F.col("end_date") < F.lit("2026-04-25").cast("date"))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
        F.concat_ws(" / ", F.col("status"), F.col("product")).alias("status_product_label"),
    )
)

screened = (
    valid.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.premium").between(2033.8, 4678.8)), "left_anti")
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.max("claim_date").alias("last_claim_date"))
)

with_stats = (
    screened.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select("s.*", "c.last_claim_date")
)

detail_rows = (
    with_stats.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "s.end_date",
        "s.status",
        "s.status_product_label",
        F.col("c.id").alias("claim_id"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.status").alias("claim_status"),
        "c.claim_type",
    )
)

result = (
    detail_rows
    .filter(F.col("holder_id").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "status",
        "status_product_label",
        "claim_id",
        "claim_claimed_amount",
        "claim_status",
        "claim_type",
        F.initcap(F.trim(F.col("status_product_label"))).alias("status_product_label_clean"),
        F.datediff(F.col("end_date"), F.col("start_date")).alias("days_start_date_to_end_date"),
    )
    .orderBy(F.col("product").desc(), F.col("claim_id").desc(), "id")
    .limit(5)
)
