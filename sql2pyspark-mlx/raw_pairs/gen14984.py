from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "inner")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "left")
    .filter(F.col("suppliers.supplier_name").like("Lio%"))
    .groupBy(
        F.when(F.col("suppliers.rating") >= 4.6, "high").when(F.col("suppliers.rating") >= 2.1, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.countDistinct(F.col("order_items.unit_price")).alias("unique_orders"),
        F.count("*").alias("order_item_count"),
    )
)
