from pyspark.sql import functions as F

inventory_by_product = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2]))
    .groupBy("product_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
)

result = (
    df_products.alias("p")
    .join(inventory_by_product.alias("I"), F.col("p.id") == F.col("I.product_id"), "inner")
    .select(F.col("p.category"), F.col("I.avg_quantity"), F.col("I.quantity_range"))
)
