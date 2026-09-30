from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "inner")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "inner")
    .filter(F.col("order_items.order_id").isin([10, 20, 32, 35]) & (F.col("suppliers.rating") <= 3.1))
    .select(
        F.col("order_items.id"),
        F.col("order_items.quantity"),
        F.col("products.launch_date"),
        F.col("products.category"),
        F.col("suppliers.rating"),
        F.length(F.col("products.category")).alias("category_length"),
    )
)
