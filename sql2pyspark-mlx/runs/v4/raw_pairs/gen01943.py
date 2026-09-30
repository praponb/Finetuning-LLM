from pyspark.sql import functions as F

kept = (
    df_products
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.product_id").isin([4, 5, 6, 10])), "left_semi")
)

result = (
    kept
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
    )
    .orderBy(F.col("stock_qty").asc_nulls_first())
)
