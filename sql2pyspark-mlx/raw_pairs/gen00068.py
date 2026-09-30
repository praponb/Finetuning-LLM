from pyspark.sql import functions as F

product_summary = (
    df_inventory
    .filter(F.col("product_id").isin([1, 7]))
    .groupBy("product_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.max("last_restock").alias("last_last_restock"),
    )
)

result = (
    df_products.alias("products")
    .join(product_summary.alias("product_summary"), F.col("products.id") == F.col("product_summary.product_id"), "inner")
    .filter(F.col("product_summary.total_quantity") > 780)
    .select(
        F.col("products.id"),
        F.col("products.supplier_id"),
        F.col("products.cost"),
        F.col("product_summary.total_quantity"),
        F.col("product_summary.last_last_restock"),
    )
    .orderBy("cost", "id")
    .limit(5)
)
