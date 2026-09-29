from pyspark.sql import functions as F

price_by_supplier_id = df_products.groupBy("supplier_id").agg(F.min("price").alias("min_product_price"))

joined = (
    df_products
    .alias("s")
    .join(df_suppliers.alias("d"), F.col("s.supplier_id").eqNullSafe(F.col("d.id")), "left")
    .select("s.*", F.col("d.rating"))
)

result = (
    joined
    .join(price_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "stock_qty",
        "launch_date",
        "supplier_id",
        "rating",
        F.col("min_product_price"),
    )
    .orderBy(F.col("supplier_id").asc_nulls_last())
)
