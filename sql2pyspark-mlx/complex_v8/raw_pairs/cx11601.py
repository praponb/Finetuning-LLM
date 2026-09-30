from pyspark.sql import functions as F

filtered_policys = df_policies.filter((F.col("holder_id") >= 7) & (F.col("product") == "TRAVEL"))

with_parent_holder = (
    filtered_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.end_date",
        F.col("p.region").alias("holder_region"),
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

screened = (
    with_parent_holder.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.status") == "EXPIRED"), "left_anti")
)

result = (
    screened
    .groupingSets(
        [["product", "holder_region"], ["product"], ["holder_region"], []],
        "product", "holder_region",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.round(F.sum("premium") / F.nullif(F.sum("holder_risk_score"), F.lit(0)), 3).alias("premium_to_holder_risk_score_ratio"),
        F.countDistinct("holder_id").alias("distinct_holder_ids"),
    )
    .orderBy(
        F.col("product").asc_nulls_last(),
        F.col("holder_region").asc_nulls_last(),
    )
)
