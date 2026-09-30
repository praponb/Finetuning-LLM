from pyspark.sql import functions as F

filtered = (
    df_products
    .filter(F.col("brand").isNotNull() & (F.col("category") != "Grocery"))
    .select(
        "*",
        F.initcap(F.trim("brand")).alias("brand_title"),
        F.round((F.col("stock_qty") * 0.85), 2).alias("stock_qty_scaled"),
        F.coalesce("brand", F.lit("OTHER")).alias("resolved_brand"),
    )
)

joined = (
    filtered
    .alias("e")
    .join(df_suppliers.alias("b"), F.col("e.supplier_id").eqNullSafe(F.col("b.id")), "left")
    .filter(F.col("b.rating") <= 3.9)
    .select(
        "e.id",
        "e.product_name",
        "e.category",
        "e.brand",
        "e.price",
        "e.cost",
        "e.launch_date",
        "e.supplier_id",
        "e.brand_title",
        "e.stock_qty_scaled",
        F.col("b.city"),
        F.col("b.rating"),
        F.col("b.supplier_name"),
    )
)

matched_keys = (
    joined
    .select("brand", "id")
    .subtract(
        df_products
        .filter(F.col("supplier_id").isin([1, 11, 28]))
        .select("brand", "id")
    )
)

verified = (
    joined
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.brand") == F.col("k.brand")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

joined_products = (
    verified
    .alias("s")
    .join(df_suppliers.alias("p2"), F.col("s.supplier_id") == F.col("p2.id"), "left")
    .select(
        "s.id",
        "s.product_name",
        "s.category",
        "s.brand",
        "s.price",
        "s.cost",
        "s.supplier_id",
        "s.city",
        "s.rating",
        F.col("p2.supplier_name").alias("supplier_supplier_name"),
        F.col("p2.city").alias("supplier_city"),
    )
)

filtered_products = (
    joined_products
    .filter(F.col("cost") >= 270)
    .select(
        "*",
        F.floor("rating").alias("rating_whole"),
        F.coalesce("city", F.lit("DIRECT")).alias("city_filled"),
    )
)

grouped = (
    filtered_products
    .withColumn("cost_tier", F.when(F.col("cost") >= 720, "HIGH").otherwise("LOW"))
    .withColumn("rating_whole_tier", F.when(F.col("rating_whole") >= 81, "PREMIUM").otherwise("STANDARD"))
    .rollup("cost_tier", "rating_whole_tier")
    .agg(
        F.count(F.when(F.col("category") == "Fashion", 1)).alias("fashion_rows"),
        F.sum(F.when(F.col("category") == "Books", F.col("rating")).otherwise(0)).alias("books_rating"),
        F.percentile_approx("price", [0.25, 0.5, 0.75]).alias("price_quartiles"),
        F.stddev_samp("price").alias("price_stddev"),
        F.grouping("cost_tier").alias("grouping_id"),
    )
    .filter(F.col("books_rating") > 0)
)

result = (
    grouped
    .select("fashion_rows", "books_rating", "price_stddev", "grouping_id")
    .orderBy(F.col("grouping_id").desc())
)
