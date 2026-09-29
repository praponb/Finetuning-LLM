from pyspark.sql import functions as F

valid_policys = (
    df_policies
    .filter(F.col("start_date").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.abs(F.col("premium") - 2844.1).alias("premium_distance"),
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
    )
)

combined_details_holder = (
    valid_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.*",
        "p.holder_name",
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

with_parent_holder = (
    combined_details_holder.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select("s.*", F.col("p.region").alias("holder_region"))
)

screened = (
    with_parent_holder.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.status").isNotNull()), "left_anti")
)

result = (
    screened
    .select(
        "id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        "premium_distance",
        "holder_name",
        "holder_risk_score",
        "holder_joined_on",
        "holder_region",
        (F.col("premium_filled") + F.col("premium_distance")).alias("premium_filled_plus_premium_distance"),
        F.substring(F.trim(F.col("holder_region")), 1, 3).alias("holder_region_prefix"),
    )
    .orderBy(F.col("product").desc_nulls_first(), "id")
    .limit(20)
)
