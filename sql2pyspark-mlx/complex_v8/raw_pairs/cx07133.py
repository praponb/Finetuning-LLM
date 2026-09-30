from pyspark.sql import functions as F

valid = (
    df_claims
    .filter((F.col("adjuster_id").isNotNull()) & (F.col("policy_id").isNotNull()))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "status",
    )
)

joined_policy = (
    valid.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.product") == "HOME"), "left")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.approved_amount",
        F.col("p.product").alias("policy_product"),
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.start_date").alias("policy_start_date"),
        F.col("a.level").alias("adjuster_level"),
        "a.adjuster_name",
    )
)

result = (
    joined_policy
    .groupingSets(
        [["policy_product", "policy_id"], ["policy_product"], []],
        "policy_product", "policy_id",
    )
    .agg(
        F.grouping("policy_product").alias("is_total_policy_product"),
        F.sum(F.when(F.lower(F.col("adjuster_name")).like("%an%"), F.col("adjuster_level")).otherwise(0)).alias("adjuster_level_lower_adjuster_name_like_an"),
    )
    .select(
        F.coalesce(F.col("policy_product"), F.lit("ALL")).alias("policy_product"),
        "policy_id",
        "is_total_policy_product",
        "adjuster_level_lower_adjuster_name_like_an",
    )
    .orderBy(
        F.col("policy_product").asc_nulls_last(),
        F.col("policy_id").asc_nulls_last(),
    )
)
