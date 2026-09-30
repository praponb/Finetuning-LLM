from pyspark.sql import functions as F

price_by_supplier_id = df_products.groupBy("supplier_id").agg(F.avg("price").alias("avg_product_price"))

joined = (
    df_products
    .alias("src")
    .join(df_suppliers.alias("b"), F.col("src.supplier_id") == F.col("b.id"), "left")
    .filter(F.col("b.supplier_name").like("%Trad%"))
    .select("src.*", F.col("b.city"), F.col("b.supplier_name"), F.col("b.rating"))
)

result = (
    joined
    .join(price_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "category",
        "brand",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        "city",
        F.col("avg_product_price"),
    )
)
