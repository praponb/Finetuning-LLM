from pyspark.sql import functions as F

filtered = (
    df_store_sales
    .filter(F.col("sale_date") > F.to_date(F.lit("2023-08-20")))
    .select("*", (F.col("revenue") * F.col("units")).alias("revenue_per_unit"))
)

enriched = (
    filtered
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.price"), F.col("p.category"))
)

valid = (
    enriched
    .filter(F.col("sale_date") <= "2024-10-01")
    .select(
        "*",
        F.round("units", 2).alias("units_rounded"),
        (F.col("price") + F.col("revenue_per_unit")).alias("price_plus_revenue_per_unit"),
    )
)

kept_keys = (
    valid
    .select("id", "product_id")
    .exceptAll(
        df_store_sales
        .filter(F.col("product_id").isin([1, 2, 10]))
        .select("id", "product_id")
    )
)

reconciled = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id") == F.col("d.id"), "inner")
    .groupBy(F.col("d.city").alias("city"), F.col("s.category").alias("category"))
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.col("s.price")).alias("total_price"),
        F.sum(F.col("s.revenue")).alias("total_revenue"),
    )
    .orderBy(F.col("city").desc_nulls_last())
)
