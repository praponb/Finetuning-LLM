from pyspark.sql import functions as F

order_items_by_product = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .groupBy("product_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.max("quantity").alias("max_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(order_items_by_product.alias("O"), F.col("p.id") == F.col("O.product_id"), "left")
    .filter(F.col("O.max_quantity") > 4.4)
    .select(
        F.col("p.launch_date"),
        F.col("p.supplier_id"),
        F.col("O.avg_quantity"),
        F.col("O.max_quantity"),
    )
)
