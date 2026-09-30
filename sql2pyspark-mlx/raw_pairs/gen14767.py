from pyspark.sql import functions as F

filtered = (
    df_products
    .filter((F.col("launch_date") >= F.to_date(F.lit("2023-08-17"))) & F.col("brand").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.array_contains("tags", "eco").alias("is_eco"),
        F.coalesce("brand", F.lit("NONE")).alias("resolved_brand"),
    )
)

matched_keys = (
    filtered
    .select("category")
    .intersect(
        df_products
        .filter(F.col("supplier_id") == 34)
        .select("category")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.category") == F.col("k.category")), "inner")
    .select("s.*")
)

not_flagged = (
    reconciled
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.unit_price") >= 330), "left_semi")
)

not_flagged_products = (
    not_flagged
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.sale_date") >= F.col("s.launch_date")), "left_anti")
)

result = (
    not_flagged_products
    .alias("s")
    .join(df_suppliers.alias("d"), F.col("s.supplier_id") == F.col("d.id"), "inner")
    .rollup(F.col("d.city").alias("city"), F.col("s.category").alias("category"))
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg(F.col("s.price")), 2).alias("avg_price"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("city").desc_nulls_last())
)
