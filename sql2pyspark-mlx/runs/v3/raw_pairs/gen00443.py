from pyspark.sql import functions as F

inventory_by_product = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.count("*").alias("num_inventory"),
    )
)

result = (
    df_products.alias("p")
    .join(inventory_by_product.alias("ibp"), F.col("p.id") == F.col("ibp.product_id"), "left")
    .filter(F.col("ibp.avg_quantity") > 25)
    .select(F.col("p.stock_qty"), F.col("ibp.avg_quantity"), F.col("ibp.num_inventory"))
)
