from pyspark.sql import functions as F

revenue_by_product_id = df_store_sales.groupBy("product_id").agg(F.avg("revenue").alias("avg_store_sale_revenue"))

kept_keys = (
    df_order_items
    .select("id", "product_id")
    .intersectAll(
        df_order_items
        .filter(F.col("unit_price") <= 300)
        .select("id", "product_id")
    )
)

kept_rows = (
    df_order_items
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .join(revenue_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.col("avg_store_sale_revenue"),
    )
    .orderBy(F.col("avg_store_sale_revenue").desc_nulls_last())
)
