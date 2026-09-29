from pyspark.sql import functions as F

filtered = (
    df_products
    .filter((F.col("launch_date") >= F.lit("2023-06-14").cast("date")) & F.col("supplier_id").isNotNull() & ~F.col("category").isin(["Books", "Grocery"]))
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
        F.round((F.col("stock_qty") / F.col("price")), 2).alias("stock_qty_per_price"),
    )
)

kept_keys = (
    filtered
    .select("id", "category")
    .subtract(
        df_products
        .filter(F.col("category").isin(["Home", "Sports"]))
        .select("id", "category")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.category") == F.col("k.category")), "inner")
    .select("s.*")
)

joined = (
    kept_rows
    .alias("src")
    .join(df_suppliers.alias("p"), F.col("src.supplier_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.rating"), F.col("p.country"), F.col("p.supplier_name"))
)

result = (
    joined
    .groupBy(
        F.when(F.col("cost") >= 620, "LARGE").otherwise("SMALL").alias("cost_tier"),
    )
    .agg(
        F.count(F.when(F.col("brand") == "Initech", 1)).alias("initech_count"),
        F.stddev_samp("stock_qty").alias("stock_qty_stddev"),
    )
)
