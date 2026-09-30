from pyspark.sql import functions as F

product_totals = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.countDistinct("quantity").alias("unique_stores"),
        F.max("last_restock").alias("last_last_restock"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.cost"),
        F.col("pt.unique_stores"),
        F.col("pt.last_last_restock"),
    )
)
