from pyspark.sql import functions as F

base_policys = (
    df_policies
    .filter(F.col("holder_id").isNotNull())
    .select(
        "id",
        "holder_id",
        "start_date",
        "end_date",
        "status",
        F.concat(F.col("product"), F.lit("#"), F.col("id").cast("string")).alias("product_ref"),
        F.when(F.col("premium") >= 2945.9, "A").when(F.col("premium") >= 1541.8, "B").otherwise("C").alias("premium_tier"),
    )
)

exploded_detail_claims = (
    base_policys.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.end_date",
        "s.premium_tier",
        F.col("c.id").alias("claim_id"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
    )
)

result = (
    exploded_detail_claims
    .filter(F.col("premium_tier").isin("C", "A"))
    .select(
        "id",
        "start_date",
        "end_date",
        "premium_tier",
        "claim_claimed_amount",
        "claim_adjuster_id",
        F.date_format(F.col("end_date"), "yyyy-MM").alias("end_date_label"),
    )
    .orderBy("claim_adjuster_id", F.col("premium_tier").desc())
)
