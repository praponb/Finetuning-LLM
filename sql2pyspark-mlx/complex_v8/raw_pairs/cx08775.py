from pyspark.sql import functions as F

prepared_policys = (
    df_policies
    .filter((F.col("premium") >= 2013.8) & (F.col("status").isin("ACTIVE", "CANCELLED")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        F.coalesce(F.col("product"), F.lit("UNKNOWN")).alias("product_or_unknown"),
    )
)

with_activity = (
    prepared_policys.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.product") == "TRAVEL"), "left_semi")
)

screened = (
    with_activity.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left_semi")
)

result = (
    screened
    .cube("product_or_unknown", "holder_id")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.count(F.when(F.col("product") == "HEALTH", 1)).alias("n_product_health"),
        F.round(F.stddev("premium"), 2).alias("stddev_premium"),
        F.sum(F.when(F.col("product") == "TRAVEL", 1).otherwise(0)).alias("n_product_travel"),
    )
    .select(
        F.coalesce(F.col("product_or_unknown"), F.lit("ALL")).alias("product_or_unknown"),
        "holder_id",
        "grouping_level",
        "n_product_health",
        "stddev_premium",
        "n_product_travel",
    )
    .orderBy(
        F.col("product_or_unknown").asc_nulls_last(),
        F.col("holder_id").asc_nulls_last(),
    )
)
