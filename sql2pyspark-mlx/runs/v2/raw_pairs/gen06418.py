from pyspark.sql import functions as F

product_totals = (
    df_inventory
    .filter(F.col("quantity") != 170)
    .groupBy("product_id")
    .agg(F.count("*").alias("inventory_count"))
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "left")
    .filter(F.col("pt.inventory_count") > 6)
    .select(F.col("p.id"), F.col("p.category"), F.col("pt.inventory_count"))
)
