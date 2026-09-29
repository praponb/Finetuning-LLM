from pyspark.sql import functions as F

inventory_by_product = (
    df_inventory
    .filter(F.col("quantity") < 229)
    .groupBy("product_id")
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.min("quantity").alias("min_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(inventory_by_product.alias("I"), F.col("p.id") == F.col("I.product_id"), "inner")
    .filter(F.col("I.min_quantity") > 120)
    .select(
        F.col("p.id"),
        F.col("p.cost"),
        F.col("p.launch_date"),
        F.col("I.max_quantity"),
        F.col("I.min_quantity"),
    )
    .orderBy("cost", "id")
    .limit(10)
)
