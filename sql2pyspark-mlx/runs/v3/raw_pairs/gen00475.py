from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.filter(F.col("sale_date") <= "2025-03-09").alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left_semi")
    .filter(F.array_contains(F.col("p.tags"), "new"))
    .select(F.col("p.id"), F.col("p.cost"), F.col("p.price"))
    .orderBy(F.col("price").desc(), "id")
)
