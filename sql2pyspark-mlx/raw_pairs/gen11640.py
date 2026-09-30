from pyspark.sql import functions as F

product_totals = (
    df_inventory
    .filter(F.col("last_restock") <= "2023-04-01")
    .groupBy("product_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.min("last_restock").alias("min_last_restock"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "left")
    .select(
        F.col("p.id"),
        F.col("p.cost"),
        F.col("pt.avg_quantity"),
        F.col("pt.min_last_restock"),
    )
)
