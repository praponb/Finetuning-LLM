from pyspark.sql import functions as F

units_by_product_id = df_store_sales.groupBy("product_id").agg(F.sum("units").alias("sum_store_sale_units"))

base = (
    df_store_sales
    .filter(F.col("sale_date") > F.to_date(F.lit("2023-02-15")))
    .withColumn("revenue_filled", F.coalesce("revenue", F.lit(0)))
)

with_dims = (
    base
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "left")
    .select(
        "src.id",
        "src.product_id",
        "src.units",
        "src.revenue",
        "src.revenue_filled",
        F.col("d.stock_qty"),
    )
)

kept_keys = (
    with_dims
    .select("product_id", "id")
    .intersectAll(
        df_store_sales
        .filter(F.month("sale_date").isin([7, 8, 9]))
        .select("product_id", "id")
    )
)

kept_rows = (
    with_dims
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_dims_store_sales = (
    kept_rows
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.stock_qty").alias("product_stock_qty"), F.col("p.price"))
)

joined = (
    with_dims_store_sales
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.supplier_id").isin([6, 11, 12, 27]))
    .select("src.*", F.col("p.brand"))
)

result = (
    joined
    .join(units_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "revenue",
        "revenue_filled",
        "stock_qty",
        "product_stock_qty",
        "price",
        F.col("sum_store_sale_units"),
    )
    .orderBy("price")
)
