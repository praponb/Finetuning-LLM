from pyspark.sql import functions as F

price_by_supplier_id = df_products.groupBy("supplier_id").agg(F.max("price").alias("max_product_price"))

remaining_keys = (
    df_products
    .select("category")
    .intersect(
        df_products
        .filter(~F.col("brand").isin(["Initech", "Stark", "Acme"]))
        .select("category")
    )
)

reconciled = (
    df_products
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.category") == F.col("k.category")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .join(price_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "brand",
        "price",
        "stock_qty",
        "supplier_id",
        F.col("max_product_price"),
    )
    .orderBy(F.col("brand").desc(), F.col("price").desc())
)
