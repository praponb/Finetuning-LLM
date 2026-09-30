from pyspark.sql import functions as F

prepared = (
    df_policies
    .filter((F.year(F.col("start_date")) == 2025) & (F.col("holder_id") >= 5))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
        F.lower(F.trim(F.col("product"))).alias("product_clean"),
        F.when(F.col("product").isin("HOME", "HEALTH"), "group_a").when(F.col("product") == "AUTO", "group_b").otherwise("other").alias("product_group"),
    )
)

with_parent = (
    prepared.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (~F.col("p.region").isin("North", "East")), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.end_date",
        "s.status",
        "s.premium_filled",
        "s.product_clean",
        "s.product_group",
        F.col("p.segment").alias("holder_segment"),
    )
)

joined_holder = (
    with_parent.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.lower(F.col("p.holder_name")).like("%rt%"))
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.status",
        "s.premium_filled",
        "s.product_clean",
        "s.holder_segment",
        "p.holder_name",
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.segment").alias("holder_segment_2"),
    )
)

result = (
    joined_holder
    .groupingSets(
        [["holder_id", "product"], ["holder_id"], []],
        "holder_id", "product",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.array_sort(F.collect_set("holder_segment_2")).alias("holder_segment_2_list"),
        F.min("premium_filled").alias("min_premium_filled"),
    )
    .select(
        "holder_id",
        F.coalesce(F.col("product"), F.lit("ALL")).alias("product"),
        "cnt",
        "holder_segment_2_list",
        "min_premium_filled",
    )
)
