from pyspark.sql import functions as F

filtered = (
    df_policies
    .filter((F.col("holder_id") >= 6) & (F.col("product").isin("AUTO", "HOME", "HEALTH")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.when(F.col("product").isin("TRAVEL", "HOME"), "group_a").when(F.col("product") == "AUTO", "group_b").otherwise("other").alias("product_group"),
    )
)

annotated = (
    filtered
    .select(
        "*",
        F.substring(F.trim(F.col("product_group")), 1, 3).alias("product_group_prefix"),
        F.month(F.col("start_date")).alias("start_date_month"),
    )
)

combined_details = (
    annotated.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.risk_score") <= 0.6)
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.status",
        "s.product_group",
        "s.start_date_month",
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

result = (
    combined_details
    .groupingSets(
        [["product", "holder_id", "start_date_month"], ["product"]],
        "product", "holder_id", "start_date_month",
    )
    .agg(F.grouping_id().alias("gid"), F.max("premium").alias("max_premium"))
    .orderBy(
        F.col("product").asc_nulls_last(),
        F.col("holder_id").asc_nulls_last(),
        F.col("start_date_month").asc_nulls_last(),
    )
)
