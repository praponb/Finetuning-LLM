from pyspark.sql import functions as F

product_totals = (
    df_inventory
    .filter(F.col("quantity").between(104, 117))
    .groupBy("product_id")
    .agg(
        F.max("last_restock").alias("latest_last_restock"),
        F.min("quantity").alias("min_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.launch_date"),
        F.col("p.supplier_id"),
        F.col("pt.latest_last_restock"),
        F.col("pt.min_quantity"),
    )
)
