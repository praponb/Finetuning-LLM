from pyspark.sql import functions as F

product_totals = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.max("last_restock").alias("latest_last_restock"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "inner")
    .select(
        F.col("p.product_name"),
        F.col("pt.quantity_count"),
        F.col("pt.latest_last_restock"),
    )
    .orderBy(F.col("quantity_count").desc(), "product_name")
)
