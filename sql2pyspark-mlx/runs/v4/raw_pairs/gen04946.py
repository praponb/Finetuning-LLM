from pyspark.sql import functions as F

kept_keys = (
    df_products
    .select("supplier_id")
    .intersectAll(
        df_products
        .filter(F.col("cost") < 460)
        .select("supplier_id")
    )
)

reconciled = (
    df_products
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.supplier_id") == F.col("k.supplier_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
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
    .orderBy(F.col("launch_date").asc_nulls_last(), "stock_qty")
)
