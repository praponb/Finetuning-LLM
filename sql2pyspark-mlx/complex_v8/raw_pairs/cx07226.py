from pyspark.sql import functions as F

cleaned = (
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
        F.concat(F.col("product"), F.lit("#"), F.col("id").cast("string")).alias("product_ref"),
        F.date_trunc("quarter", F.col("end_date")).alias("end_date_quarter"),
    )
)

qualified = (
    cleaned.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (~F.col("p.status").isin("EXPIRED", "LAPSED")), "left_semi")
    .filter(F.col("end_date") >= F.lit("2026-01-23").cast("date"))
)

qualified_2 = (
    qualified.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (~F.col("c.status").isin("PAID", "REJECTED")) & (F.col("c.approved_amount") < 19337.6), "left_anti")
)

with_activity = (
    qualified_2.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claim_type").isin("flood", "delay")), "left_anti")
)

screened = (
    with_activity.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.premium") >= 2013.8), "left_anti")
    .filter(F.col("premium") >= 1051.9)
)

result = (
    screened
    .rollup("holder_id", "product", "status")
    .agg(F.sum("premium").alias("total_premium"))
    .select(
        "holder_id",
        F.coalesce(F.col("product"), F.lit("ALL")).alias("product"),
        F.coalesce(F.col("status"), F.lit("ALL")).alias("status"),
        "total_premium",
    )
    .orderBy(
        F.col("holder_id").asc_nulls_last(),
        F.col("product").asc_nulls_last(),
        F.col("status").asc_nulls_last(),
    )
)
