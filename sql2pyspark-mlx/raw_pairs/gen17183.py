from pyspark.sql import functions as F

price_by_supplier_id = df_products.groupBy("supplier_id").agg(F.max("price").alias("max_product_price"))

remaining_keys = (
    df_products
    .select("supplier_id", "brand")
    .intersectAll(
        df_products
        .filter(F.col("stock_qty").between(173, 189))
        .select("supplier_id", "brand")
    )
)

verified = (
    df_products
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.supplier_id") == F.col("k.supplier_id")) & (F.col("s.brand") == F.col("k.brand")), "inner")
    .select("s.*")
)

result = (
    verified
    .join(price_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "category",
        "brand",
        "price",
        "stock_qty",
        "launch_date",
        "supplier_id",
        F.col("max_product_price"),
    )
    .orderBy(F.col("category").desc_nulls_last())
)
