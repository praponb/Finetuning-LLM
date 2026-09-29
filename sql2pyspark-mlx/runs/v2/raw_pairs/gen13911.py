from pyspark.sql import functions as F

product_totals = (
    df_inventory
    .groupBy("product_id")
    .agg(F.max("quantity").alias("max_quantity"))
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "inner")
    .filter(F.col("pt.max_quantity") > 110)
    .select(F.col("p.id"), F.col("p.cost"), F.col("pt.max_quantity"))
    .orderBy(F.col("id").desc(), F.col("max_quantity").desc())
    .limit(20)
)
