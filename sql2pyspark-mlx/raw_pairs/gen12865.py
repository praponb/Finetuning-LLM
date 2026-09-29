from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "left")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "left")
    .groupBy(F.col("products.category"))
    .agg(
        F.round(F.stddev(F.col("order_items.unit_price")), 2).alias("stddev_unit_price"),
        F.min(F.col("order_items.unit_price")).alias("min_unit_price"),
    )
    .orderBy(F.col("min_unit_price").desc())
)
