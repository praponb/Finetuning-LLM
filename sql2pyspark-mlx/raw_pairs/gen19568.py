from pyspark.sql import functions as F

quantity_by_product_id = df_order_items.groupBy("product_id").agg(F.min("quantity").alias("min_order_item_quantity"))

kept_keys = (
    df_store_sales
    .select("product_id", "id")
    .intersectAll(
        df_store_sales
        .filter(F.col("units") < 19)
        .select("product_id", "id")
    )
)

kept_rows = (
    df_store_sales
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.col("min_order_item_quantity"),
    )
)
