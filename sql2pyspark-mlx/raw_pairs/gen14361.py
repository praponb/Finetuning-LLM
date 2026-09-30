from pyspark.sql import functions as F

stock_qty_by_supplier_id = df_products.groupBy("supplier_id").agg(F.max("stock_qty").alias("max_product_stock_qty"))

joined = (
    df_products
    .alias("src")
    .join(df_suppliers.alias("s"), F.col("src.supplier_id").eqNullSafe(F.col("s.id")), "left")
    .select("src.*", F.col("s.supplier_name"), F.col("s.rating"), F.col("s.country"))
)

not_flagged = (
    joined
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.order_id").isin([10, 24, 26])), "left_anti")
)

enriched = (
    not_flagged
    .alias("s")
    .join(df_suppliers.alias("d"), F.col("s.supplier_id").eqNullSafe(F.col("d.id")), "inner")
    .select("s.*", F.col("d.city"))
)

eligible = (
    enriched
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.order_id").isin([6, 18, 27, 35])), "left_anti")
)

result = (
    eligible
    .join(stock_qty_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "category",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        "rating",
        "country",
        F.col("max_product_stock_qty"),
    )
    .orderBy("id")
)
