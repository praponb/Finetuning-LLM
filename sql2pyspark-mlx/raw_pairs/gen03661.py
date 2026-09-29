from pyspark.sql import functions as F

stock_qty_by_supplier_id = df_products.groupBy("supplier_id").agg(F.sum("stock_qty").alias("sum_product_stock_qty"))

matched_keys = (
    df_products
    .select("id", "category")
    .intersectAll(
        df_products
        .filter(F.array_contains("tags", "sale"))
        .select("id", "category")
    )
)

kept_rows = (
    df_products
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.category") == F.col("k.category")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .join(stock_qty_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "cost",
        "launch_date",
        "supplier_id",
        F.when(F.col("product_name").rlike("^(Backpack XL|Yoga Mat)$"), "GROUP_A").when(F.col("product_name").rlike("^(Laptop Pro 14|wireless mouse)$"), "GROUP_B").otherwise("EXTENDED").alias("product_name_zone"),
        F.col("sum_product_stock_qty"),
    )
    .orderBy(F.col("launch_date").asc_nulls_first())
)
