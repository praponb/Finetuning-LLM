from pyspark.sql import functions as F

inventory_by_product = (
    df_inventory
    .groupBy("product_id")
    .agg(F.min("quantity").alias("min_quantity"))
)

result = (
    df_products.alias("p")
    .join(inventory_by_product.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .filter(F.col("t.min_quantity") > 90)
    .select(
        F.col("p.id"),
        F.col("p.launch_date"),
        F.col("p.cost"),
        F.col("t.min_quantity"),
    )
)
