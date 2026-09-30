from pyspark.sql import functions as F

product_totals = (
    df_store_sales
    .filter(F.col("revenue").between(1500, 1800))
    .groupBy("product_id")
    .agg(F.count("*").alias("cnt"))
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .filter(F.col("t.cnt") >= 8)
    .select(F.col("p.price"), F.col("p.brand"), F.col("t.cnt"))
    .orderBy(F.col("cnt").desc())
)
