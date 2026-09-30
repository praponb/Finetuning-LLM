from pyspark.sql import functions as F

revenue_by_product_id = df_store_sales.groupBy("product_id").agg(F.min("revenue").alias("min_store_sale_revenue"))

kept_keys = (
    df_order_items
    .select("id", "product_id")
    .intersectAll(
        df_order_items
        .filter(F.col("product_id").isin([3, 13, 39]))
        .select("id", "product_id")
    )
)

reconciled = (
    df_order_items
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .join(revenue_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        F.col("min_store_sale_revenue"),
    )
    .orderBy(F.col("quantity").desc())
)
