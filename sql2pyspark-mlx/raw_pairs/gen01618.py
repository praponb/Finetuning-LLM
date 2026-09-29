from pyspark.sql import functions as F

inventory_by_product = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.avg("quantity").alias("avg_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(inventory_by_product.alias("t"), F.col("p.id") == F.col("t.product_id"), "left")
    .filter(F.col("t.avg_quantity") >= 200)
    .select(
        F.col("p.id"),
        F.col("p.price"),
        F.col("t.max_quantity"),
        F.col("t.avg_quantity"),
    )
    .orderBy("avg_quantity")
)
