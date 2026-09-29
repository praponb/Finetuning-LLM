from pyspark.sql import functions as F

product_summary = (
    df_inventory
    .filter(F.col("quantity").between(180, 226))
    .groupBy("product_id")
    .agg(
        F.count("*").alias("inventory_count"),
        F.min("last_restock").alias("first_last_restock"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .select(
        F.col("p.id"),
        F.col("p.stock_qty"),
        F.col("p.brand"),
        F.col("ps.inventory_count"),
        F.col("ps.first_last_restock"),
    )
    .orderBy(F.col("inventory_count").desc())
)
