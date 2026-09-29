from pyspark.sql import functions as F

remaining_keys = (
    df_products
    .select("id", "supplier_id")
    .intersect(
        df_products
        .filter(F.col("supplier_id") == 17)
        .select("id", "supplier_id")
    )
)

verified = (
    df_products
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.supplier_id") == F.col("k.supplier_id")), "inner")
    .select("s.*")
)

result = (
    verified
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
)
