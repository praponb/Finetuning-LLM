from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "inner")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "inner")
    .select(
        F.col("order_items.unit_price"),
        F.col("products.category"),
        F.col("suppliers.city"),
        F.col("suppliers.country"),
        F.round((F.col("products.cost") * 0.9), 2).alias("cost_adjusted"),
    )
    .orderBy(F.col("cost_adjusted").desc())
)
