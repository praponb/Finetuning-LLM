from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "left")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "inner")
    .groupBy(F.col("products.category"))
    .agg(
        F.sum(F.col("order_items.unit_price")).alias("total_unit_price"),
        F.countDistinct(F.col("order_items.quantity")).alias("unique_orders"),
    )
    .orderBy("total_unit_price", "category")
)
