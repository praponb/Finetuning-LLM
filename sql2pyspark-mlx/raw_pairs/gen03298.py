from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "left")
    .join(df_suppliers.alias("suppliers"), F.col("suppliers.id") == F.col("products.supplier_id"), "inner")
    .select(
        F.col("order_items.id"),
        F.col("order_items.unit_price"),
        F.col("products.cost"),
        F.col("products.stock_qty"),
        F.col("suppliers.country"),
        F.col("suppliers.city"),
    )
    .orderBy("city")
)
