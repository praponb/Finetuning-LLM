from pyspark.sql import functions as F

product_totals = (
    df_inventory
    .filter(F.col("store_id") == 1)
    .groupBy("product_id")
    .agg(F.sum("quantity").alias("total_quantity"))
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("P"), F.col("p.id") == F.col("P.product_id"), "inner")
    .filter(F.col("P.total_quantity") >= 1130)
    .select(F.col("p.product_name"), F.col("p.cost"), F.col("P.total_quantity"))
    .orderBy("product_name")
)
