from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "left")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "inner")
    .filter(F.col("suppliers.rating").isNull())
    .groupBy(F.col("suppliers.city"))
    .agg(
        (F.max(F.col("order_items.unit_price")) - F.min(F.col("order_items.unit_price"))).alias("unit_price_range"),
        F.round(F.stddev(F.col("order_items.quantity")), 2).alias("stddev_quantity"),
    )
)
