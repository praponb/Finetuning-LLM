from pyspark.sql import functions as F

matched = (
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
        "c.claim_type",
        "c.status",
        "p.product",
        "p.premium",
    )
)

tagged = (
    matched
    .withColumn("premium_per_claimed_amount", F.round(F.col("premium") / F.nullif(F.col("claimed_amount"), F.lit(0)), 3))
)

result = (
    tagged
    .groupingSets(
        [["status", "product", "claim_type"], ["status"], ["product"], []],
        "status", "product", "claim_type",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.array_sort(F.collect_set("status")).alias("status_list"),
    )
    .select(
        F.coalesce(F.col("status"), F.lit("ALL")).alias("status"),
        F.coalesce(F.col("product"), F.lit("ALL")).alias("product"),
        F.coalesce(F.col("claim_type"), F.lit("ALL")).alias("claim_type"),
        "gid",
        "status_list",
    )
    .orderBy(
        F.col("status").asc_nulls_last(),
        F.col("product").asc_nulls_last(),
        F.col("claim_type").asc_nulls_last(),
    )
)
