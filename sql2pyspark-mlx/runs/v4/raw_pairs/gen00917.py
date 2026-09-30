from pyspark.sql import functions as F

product_totals = (
    df_inventory
    .filter(F.col("product_id") == 8)
    .groupBy("product_id")
    .agg(F.sum("quantity").alias("total_quantity"))
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "left")
    .select(F.col("p.id"), F.col("p.brand"), F.col("pt.total_quantity"))
    .orderBy("id")
)
