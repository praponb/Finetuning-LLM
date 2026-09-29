from pyspark.sql import functions as F

base = (
    df_inventory
    .filter(F.col("quantity").between(194, 222))
    .select("*", F.round("quantity", 1).alias("quantity_rounded"))
)

with_dims = (
    base
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.product_name"), F.col("p.stock_qty"), F.col("p.price"))
)

enriched = (
    with_dims
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "left")
    .select(
        "s.*",
        F.col("d.stock_qty").alias("product_stock_qty"),
        F.col("d.category"),
        F.col("d.brand"),
    )
)

enriched_inventory = (
    enriched
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id") == F.col("d.id"), "left")
    .select(
        "e.id",
        "e.product_id",
        "e.quantity",
        "e.quantity_rounded",
        "e.price",
        "e.category",
        F.col("d.product_name").alias("product_product_name"),
        F.col("d.price").alias("product_price"),
    )
)

filtered = (
    enriched_inventory
    .filter(F.col("category").isNotNull())
    .select(
        "*",
        F.substring("product_product_name", 1, 3).alias("product_product_name_prefix"),
        F.least("price", "quantity").alias("least_price_quantity"),
        F.coalesce("category", F.lit("DIRECT")).alias("resolved_category"),
    )
)

result = (
    filtered
    .cube(
        F.when(F.col("price") >= 940, "PREMIUM").otherwise("STANDARD").alias("price_tier"),
    )
    .agg(
        F.round(F.avg("product_price"), 2).alias("avg_product_price"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.count(F.when(F.col("category") == "Fashion", 1)).alias("fashion_rows"),
    )
    .orderBy(F.col("avg_product_price").desc())
)
