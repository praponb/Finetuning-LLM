from pyspark.sql import functions as F

product_stats = (
    df_inventory
    .groupBy("product_id")
    .agg(F.min("last_restock").alias("first_last_restock"))
)

result = (
    df_products.alias("products")
    .join(product_stats.alias("t"), F.col("products.id") == F.col("t.product_id"), "inner")
    .select(
        F.col("products.id"),
        F.col("products.cost"),
        F.col("products.price"),
        F.col("t.first_last_restock"),
    )
    .orderBy("first_last_restock", F.col("price").desc(), "id")
    .limit(10)
)
