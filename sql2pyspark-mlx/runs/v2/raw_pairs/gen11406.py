from pyspark.sql import functions as F

inventory_by_product = (
    df_inventory
    .filter(F.col("store_id").isin([3, 4, 6]))
    .groupBy("product_id")
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.count("quantity").alias("quantity_count"),
    )
)

result = (
    df_products.alias("p")
    .join(inventory_by_product.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .filter(F.col("t.quantity_count") > 6)
    .select(
        F.col("p.id"),
        F.col("p.cost"),
        F.col("p.brand"),
        F.col("t.stddev_quantity"),
        F.col("t.quantity_count"),
    )
    .orderBy(F.col("cost").desc())
)
