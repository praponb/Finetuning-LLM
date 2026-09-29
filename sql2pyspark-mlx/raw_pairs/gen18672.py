from pyspark.sql import functions as F

product_totals = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.count("quantity").alias("quantity_count"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "inner")
    .filter(F.col("pt.total_quantity") > 940)
    .select(
        F.col("p.id"),
        F.col("p.product_name"),
        F.col("pt.total_quantity"),
        F.col("pt.quantity_count"),
    )
)
