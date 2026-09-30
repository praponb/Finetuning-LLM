from pyspark.sql import functions as F

product_totals = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .groupBy("product_id")
    .agg(F.min("quantity").alias("min_quantity"))
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("t"), F.col("p.id") == F.col("t.product_id"), "left")
    .filter(F.col("t.min_quantity") > 8.1)
    .select(F.col("p.category"), F.col("t.min_quantity"))
)
