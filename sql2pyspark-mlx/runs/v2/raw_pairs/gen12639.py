from pyspark.sql import functions as F

inventory_by_product = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
)

result = (
    df_products.alias("p")
    .join(inventory_by_product.alias("ibp"), F.col("p.id") == F.col("ibp.product_id"), "inner")
    .select(
        F.col("p.supplier_id"),
        F.col("p.stock_qty"),
        F.col("ibp.stddev_quantity"),
        F.col("ibp.quantity_range"),
    )
)
